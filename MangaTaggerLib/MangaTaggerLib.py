import itertools
import logging
import os
import re
import shutil
import time
from datetime import datetime
from os import path
from pathlib import Path

import pymanga
from fuzzywuzzy import fuzz
from requests.exceptions import ConnectionError
from xml.etree.ElementTree import SubElement, Element, Comment, tostring
from xml.dom.minidom import parseString
from zipfile import ZipFile

from jikanpy.exceptions import APIException

from googletrans import Translator
from MangaTaggerLib.api import MTJikan, AniList, Kitsu, MangaUpdates, NH, Fakku
from MangaTaggerLib.database import MetadataTable, ProcFilesTable, ProcSeriesTable
from MangaTaggerLib.errors import FileAlreadyProcessedError, FileUpdateNotRequiredError, UnparsableFilenameError, \
    MangaNotFoundError, MangaMatchedException
from MangaTaggerLib.models import Metadata, Data
from MangaTaggerLib.task_queue import QueueWorker
from MangaTaggerLib.thumbnail import thumb
from MangaTaggerLib.utils import AppSettings, compare

# Global Variable Declaration
LOG = logging.getLogger('MangaTaggerLib.MangaTaggerLib')

CURRENTLY_PENDING_DB_SEARCH = set()
CURRENTLY_PENDING_RENAME = set()

preferences = ["AniList", "MangaUpdates", "MAL", "Fakku", "NHentai"]


def main():
    AppSettings.load()
    QueueWorker.run()


def process_manga_chapter(file_path: Path, event_id, download_dir):
    filename = file_path.name
    directory_path = file_path.parent
    directory_name = file_path.parent.name

    logging_info = {
        'event_id': event_id,
        'manga_title': directory_name,
        "original_filename": filename
    }

    LOG.info(f'Now processing "{file_path}"...', extra=logging_info)

    LOG.debug(f'filename: {filename}')
    LOG.debug(f'directory_path: {directory_path}')
    LOG.debug(f'directory_name: {directory_name}')

    if directory_path == download_dir:
        manga_details = file_renamer(filename, None, logging_info)
        directory_name = manga_details[2]
    else:
        manga_details = file_renamer(filename, directory_name, logging_info)

    try:
        new_filename = manga_details[0]
        LOG.debug(f'new_filename: {new_filename}')
    except TypeError:
        LOG.warning(f'Manga Tagger was unable to process "{file_path}"', extra=logging_info)
        return None

    manga_library_dir = Path(AppSettings.library_dir, directory_name)
    LOG.debug(f'Manga Library Directory: {manga_library_dir}')

    if not manga_library_dir.exists():
        LOG.info(f'A directory for "{directory_name}" in "{AppSettings.library_dir}" does not exist; creating now.')
        manga_library_dir.mkdir()

    new_file_path = Path(manga_library_dir, new_filename)
    LOG.debug(f'new_file_path: {new_file_path}')

    LOG.info(f'Checking for current and previously processed files with filename "{new_filename}"...',
             extra=logging_info)

    if AppSettings.mode_settings is None or AppSettings.mode_settings['rename_file']:
        try:
            # Multithreading Optimization
            if new_file_path in CURRENTLY_PENDING_RENAME:
                LOG.info(f'A file is currently being renamed under the filename "{new_filename}". Locking '
                         f'{file_path} from further processing until this rename action is complete...',
                         extra=logging_info)

                while new_file_path in CURRENTLY_PENDING_RENAME:
                    time.sleep(1)

                LOG.info(f'The file being renamed to "{new_file_path}" has been completed. Unlocking '
                         f'"{new_filename}" for file rename processing.', extra=logging_info)
            else:
                LOG.info(f'No files currently currently being processed under the filename '
                         f'"{new_filename}". Locking new filename for processing...', extra=logging_info)
                CURRENTLY_PENDING_RENAME.add(new_file_path)

            rename_action(file_path, new_file_path, directory_name, manga_details[1], logging_info)
        except (FileExistsError, FileUpdateNotRequiredError, FileAlreadyProcessedError) as e:
            LOG.exception(e, extra=logging_info)
            CURRENTLY_PENDING_RENAME.remove(new_file_path)
            return

    # More Multithreading Optimization
    if directory_name in ProcSeriesTable.processed_series:
        LOG.info(f'"{directory_name}" has been processed as a searched series and will continue processing.',
                 extra=logging_info)
    else:
        if directory_name in CURRENTLY_PENDING_DB_SEARCH:
            LOG.info(f'"{directory_name}" has not been processed as a searched series but is currently pending '
                     f'a database search. Suspending further processing until database search has finished...',
                     extra=logging_info)

            while directory_name in CURRENTLY_PENDING_DB_SEARCH:
                time.sleep(1)

            LOG.info(f'"{directory_name}" has been processed as a searched series and will now be unlocked for '
                     f'processing.', extra=logging_info)
        else:
            LOG.info(f'"{directory_name}" has not been processed as a searched series nor is it currently pending '
                     f'a database search. Locking series from being processed until database has been searched...',
                     extra=logging_info)
            CURRENTLY_PENDING_DB_SEARCH.add(directory_name)

    try:
        metadata_tagger(directory_name, manga_details[1], manga_details[2], logging_info, new_file_path, file_path)

    except MangaNotFoundError:
        LOG.info(f'Processing on "{new_file_path}" has failed.', extra=logging_info)
        error_folder_path = Path(manga_library_dir, "No Match")
        if not os.path.isdir(error_folder_path):
            os.mkdir(error_folder_path)
        shutil.move(new_file_path, Path(error_folder_path, new_file_path.parts[-1]))

    except Exception:
        LOG.info(f'Processing on "{new_file_path}" has failed.', extra=logging_info)
        error_folder_path = Path(manga_library_dir, "Exception")
        if not os.path.isdir(error_folder_path):
            os.mkdir(error_folder_path)
        shutil.move(new_file_path, Path(error_folder_path, new_file_path.parts[-1]))

    LOG.info(f'Processing on "{new_file_path}" has finished.', extra=logging_info)


def file_renamer(filename, manga_title, logging_info):
    LOG.info(f'Attempting to rename "{filename}"...', extra=logging_info)

    delimiters = ["chapter", "ch.", "ch", "act"]
    volumedelimiters = ["volume", "vol.", "vol"]
    filename = filename.replace(".cbz", "").title()
    if filename.find('-.-') != -1:
        split_filename = [x.strip() for x in filename.split('-.-')]
        LOG.debug(f'Chapter text for {filename}.cbz is assumed to be {split_filename}')
        if split_filename:
            filename = split_filename[1]
        else:
            filename = ""
        if manga_title is None and split_filename[0]:
            LOG.debug(f'File was in download directory, manga name assumed to be {split_filename[0]}')
            manga_title = split_filename[0]
    for x in delimiters:
        if re.search("([ ]|^)" + x + "([0-9. ])", filename.lower(), flags=re.IGNORECASE):
            LOG.debug(f'Chapter delimiter {x} was found in string {filename}')
            vol_num = None
            text = re.split(x, filename, maxsplit=1, flags=re.IGNORECASE)
            for y in volumedelimiters:
                if re.search("([ ]|^)" + y + "([0-9. ])", text[0], flags=re.IGNORECASE):
                    LOG.debug(f'Volume delimiter {y} was found in string {text[0]}')
                    volume = re.split(y, text[0], flags=re.IGNORECASE)[1]
                    vol_num = re.search(r'[\d.]+', volume).group(0)
                    LOG.debug(f'Volume #: {vol_num}')
                    break
            chapter_text = text[1].strip()
            LOG.debug(f'Chapter # + title should be: {chapter_text}')
            if re.search(r'[\d.]+', chapter_text) is None:
                LOG.debug(f'Chapter # was not found. Checking for another chapter delimiter')
                continue
            ch_num = re.search(r'[\d.]+', chapter_text).group(0)
            ch_num = ch_num.lstrip("0") or "0"
            LOG.debug(f'Chapter #: {ch_num}')
            chapter_title = re.split(r'[\d.]+', chapter_text, maxsplit=1)[1].strip()
            LOG.debug(f'Chapter title: {chapter_title}')
            if not chapter_title:
                return [f"Chapter {ch_num}.cbz", ch_num, manga_title]
            if vol_num:
                return [f"Vol. {vol_num} Chapter {ch_num}.cbz", ch_num, chapter_title]
            else:
                return [f"Chapter {ch_num}.cbz", ch_num, chapter_title]
    if 'oneshot' in filename.lower():
        LOG.debug(f'manga_type: Oneshot')
        return [f'{manga_title}.cbz', '0', manga_title]

    if filename.strip().isdigit():
        LOG.debug(f'File name is a number')
        return [f'{filename}.cbz', f'{filename}', manga_title]

    if manga_title is None:
        manga_title = filename

    logging_info['new_filename'] = filename

    LOG.info(f'File will be renamed to "{filename}".', extra=logging_info)

    return ["000.cbz", "0", manga_title]


def rename_action(current_file_path: Path, new_file_path: Path, manga_title, chapter_number, logging_info):
    chapter_number = chapter_number.replace('.', '-')
    results = ProcFilesTable.search(manga_title, chapter_number)
    LOG.debug(f'Results: {results}')

    # If the series OR the chapter has not been processed
    if results is None:
        LOG.info(f'"{manga_title}" chapter {chapter_number} has not been processed before. '
                 f'Proceeding with file rename...', extra=logging_info)
        ProcFilesTable.insert_record_and_rename(current_file_path, new_file_path, manga_title, chapter_number,
                                                logging_info)
    else:
        versions = ['v2', 'v3', 'v4', 'v5']

        existing_old_filename = results['old_filename']
        existing_current_filename = results['new_filename']

        # If currently processing file has the same name as an existing file
        if existing_current_filename == new_file_path.name:
            # If currently processing file has a version in it's filename
            if any(version in current_file_path.name.lower() for version in versions):
                # If the version is newer than the existing file
                if compare_versions(existing_old_filename, current_file_path.name):
                    LOG.info(f'Newer version of "{manga_title}" chapter {chapter_number} has been found. Deleting '
                             f'existing file and proceeding with file rename...', extra=logging_info)
                    new_file_path.unlink()
                    LOG.info(f'"{new_file_path.name}" has been deleted! Proceeding to rename new file...',
                             extra=logging_info)
                    ProcFilesTable.update_record_and_rename(results, current_file_path, new_file_path, logging_info)
                else:
                    LOG.warning(f'"{current_file_path.name}" was not renamed due being the exact same as the '
                                f'existing chapter; file currently being processed will be deleted',
                                extra=logging_info)
                    current_file_path.unlink()
                    raise FileUpdateNotRequiredError(current_file_path.name)
            # If the current file doesn't have a version in it's filename, but the existing file does
            elif any(version in existing_old_filename.lower() for version in versions):
                LOG.warning(f'"{current_file_path.name}" was not renamed due to not being an updated version '
                            f'of the existing chapter; file currently being processed will be deleted',
                            extra=logging_info)
                current_file_path.unlink()
                raise FileUpdateNotRequiredError(current_file_path.name)
            # If all else fails
            else:
                LOG.warning(f'No changes have been found for "{existing_current_filename}"; file currently being '
                            f'processed will be deleted', extra=logging_info)
                current_file_path.unlink()
                raise FileAlreadyProcessedError(current_file_path.name)

    LOG.info(f'"{new_file_path.name}" will be unlocked for any pending processes.', extra=logging_info)
    CURRENTLY_PENDING_RENAME.remove(new_file_path)


def compare_versions(old_filename: str, new_filename: str):
    old_version = 0
    new_version = 0

    LOG.debug('Preprocessing')
    LOG.debug(f'Old Version: {old_version}')
    LOG.debug(f'New Version: {new_version}')

    if 'v2' in old_filename.lower():
        old_version = 2
    elif 'v3' in old_filename.lower():
        old_version = 3
    elif 'v4' in old_filename.lower():
        old_version = 4
    elif 'v5' in old_filename.lower():
        old_version = 5

    if 'v2' in new_filename.lower():
        new_version = 2
    elif 'v3' in new_filename.lower():
        new_version = 3
    elif 'v4' in new_filename.lower():
        new_version = 4
    elif 'v5' in new_filename.lower():
        new_version = 5

    LOG.debug('Postprocessing')
    LOG.debug(f'Old Version: {old_version}')
    LOG.debug(f'New Version: {new_version}')

    if new_version > old_version:
        return True
    else:
        return False


def metadata_tagger(manga_title, manga_chapter_number, manga_chapter_title, logging_info, manga_file_path=None, old_file_path=None):
    manga_search = None
    db_exists = False

    LOG.info(f'Table search value is "{manga_title}"', extra=logging_info)

    for x in range(4):
        manga_search = dbSearch(manga_title, x)
        if manga_search is not None:
            db_exists = True
            break
    # Metadata already exists
    if db_exists:
        if manga_title in ProcSeriesTable.processed_series:
            LOG.info(f'Found an entry in manga_metadata for "{manga_title}".', extra=logging_info)
        else:
            LOG.info(f'Found an entry in manga_metadata for "{manga_title}"; unlocking series for processing.',
                     extra=logging_info)
            ProcSeriesTable.processed_series.add(manga_title)
            CURRENTLY_PENDING_DB_SEARCH.remove(manga_title)

        manga_metadata = Metadata(manga_title, logging_info, db_details=manga_search)
        logging_info['metadata'] = manga_metadata.__dict__
    # Get metadata
    else:
        sources = {
            "MAL": MTJikan(),
            "AniList": AniList(),
            "MangaUpdates": MangaUpdates(),
            "NHentai": NH(),
            "Fakku": Fakku()}
        # sources["Kitsu"] = Kitsu
        results = {}
        metadata = None
        try:
            results["MAL"] = sources["MAL"].search('manga', manga_title)
        except:
            results["MAL"] = []
            pass
        results["AniList"] = sources["AniList"].search(manga_title, logging_info)
        results["MangaUpdates"] = sources["MangaUpdates"].search(manga_title)
        results["NHentai"] = sources["NHentai"].search(manga_title)
        results["Fakku"] = sources["Fakku"].search(manga_title)
        try:
            for source in preferences:
                for result in results[source]:
                    if source == "AniList":
                        # Construct Anilist XML
                        titles = [x[1] for x in result["title"].items() if x[1] is not None]
                        [titles.append(x) for x in result["synonyms"]]
                        for title in titles:
                            if compare(manga_title, title) >= 0.9:
                                manga = sources["AniList"].manga(result["id"], logging_info)
                                manga["source"] = "AniList"
                                metadata = Data(manga, manga_title)
                                raise MangaMatchedException("Found a match")
                    elif source == "MangaUpdates":
                        # Construct MangaUpdates XML
                        if compare(manga_title, result['title']) >= 0.9:
                            manga = sources["MangaUpdates"].series(result["id"])
                            manga["source"] = "MangaUpdates"
                            metadata = Data(manga, manga_title, result["id"])
                            raise MangaMatchedException("Found a match")
                    elif source == "MAL":
                        if compare(manga_title, result['title']) >= 0.9:
                            try:
                                manga = sources["MAL"].manga(result["mal_id"])
                            except (APIException, ConnectionError) as e:
                                LOG.warning(e, extra=logging_info)
                                LOG.warning(
                                    'Manga Tagger has unintentionally breached the API limits on Jikan. Waiting 60s to clear '
                                    'all rate limiting limits...')
                                time.sleep(60)
                                manga = MTJikan().manga(result["mal_id"])
                            manga["source"] = "MAL"
                            metadata = Data(manga, manga_title, result["mal_id"])
                            raise MangaMatchedException("Found a match")
                    elif source == "Fakku":
                        if result["success"]:
                            manga = sources["Fakku"].manga(result["url"])
                            manga["source"] = "Fakku"
                            metadata = Data(manga, manga_title)
                            raise MangaMatchedException("Found a match")
                    elif source == "NHentai":
                        filenametoolong = False
                        if len(old_file_path.absolute().__str__()) == 259:
                            if fuzz.partial_ratio(manga_title, result["title"]) == 100:
                                filenametoolong = True
                        if compare(manga_title, result["title"]) >= 0.8 or filenametoolong:
                            manga = sources["NHentai"].manga(result["id"], result["title"])
                            manga["source"] = "NHentai"
                            metadata = Data(manga, manga_title, result["id"])
                            raise MangaMatchedException("Found a match")
            formats = [(r"(\w)([A-Z])", r"\1 \2"), (r"[ ][,]", ","), (r"[.]", ""), (r"([^ ]+)[']([^ ]+)", ""), (r"([^ ]+)[.]([^ ]+)", ""), (r"[ ][-]([^ ]+)", r" \1")]
            for x in range(len(formats)):
                combinations = itertools.combinations(formats, x+1)
                for y in combinations:
                    for z in y:
                        formatted = manga_title
                        formatted = re.sub(z[0],z[1], formatted)
                        formattedresults = sources["NHentai"].search(formatted)
                        for formattedresult in formattedresults:
                            if compare(manga_title, formattedresult["title"]) >= 0.8:
                                manga = sources["NHentai"].manga(formattedresult["id"], formattedresult["title"])
                                manga["source"] = "NHentai"
                                metadata = Data(manga, manga_title, formattedresult["id"])
                                raise MangaMatchedException("Found a match")
            raise MangaNotFoundError(manga_title)
        except MangaNotFoundError as mnfe:
            LOG.exception(mnfe, extra=logging_info)
            raise
        except MangaMatchedException:
            pass

        manga_metadata = Metadata(manga_title, logging_info, details=metadata.toDict())
        logging_info['metadata'] = manga_metadata.__dict__

        if AppSettings.mode_settings is None or ('database_insert' in AppSettings.mode_settings.keys()
                                                 and AppSettings.mode_settings['database_insert']):
            MetadataTable.insert(manga_metadata, logging_info)

        LOG.info(f'Retrieved metadata for "{manga_title}" from the Anilist and MyAnimeList APIs; '
                 f'now unlocking series for processing!', extra=logging_info)
        ProcSeriesTable.processed_series.add(manga_title)
        CURRENTLY_PENDING_DB_SEARCH.remove(manga_title)

    if AppSettings.mode_settings is None or ('write_comicinfo' in AppSettings.mode_settings.keys()
                                             and AppSettings.mode_settings['write_comicinfo']):
        manga_metadata.title = manga_chapter_title
        comicinfo_xml = construct_comicinfo_xml(manga_metadata, manga_chapter_number, logging_info)
        reconstruct_manga_chapter(comicinfo_xml[0], manga_file_path, comicinfo_xml[1], logging_info)

    return manga_metadata

def construct_comicinfo_xml(metadata, chapter_number, logging_info):
    LOG.info(f'Constructing comicinfo object for "{metadata.series_title}", chapter {chapter_number}...',
             extra=logging_info)

    comicinfo = Element('ComicInfo')

    application_tag = Comment('Generated by Manga Tagger, an Endless Galaxy Studios project')
    comicinfo.append(application_tag)

    title = SubElement(comicinfo, 'Title')
    title.text = metadata.title

    series = SubElement(comicinfo, 'Series')
    series.text = metadata.series_title

    alt_series = SubElement(comicinfo, 'AlternateSeries')
    if metadata.series_title and metadata.series_title.strip():
        series_title_lang = Translator().detect(metadata.series_title).lang
    if metadata.series_title_eng and series_title_lang == "ja":
        alt_series.text = metadata.series_title_eng
    elif metadata.series_title_jap and series_title_lang == "en":
        alt_series.text = metadata.series_title_jap

    if not alt_series.text and metadata.synonyms:
        alt_series.text = tryIter(metadata.synonyms)

    number = SubElement(comicinfo, 'Number')
    number.text = f'{chapter_number}'

    summary = SubElement(comicinfo, 'Summary')
    summary.text = metadata.description

    page_count = SubElement(comicinfo, 'PageCount')
    if metadata.page_count:
        page_count.text = str(metadata.page_count)

    if metadata.publish_date:
        publish_date = datetime.strptime(metadata.publish_date, '%Y-%m-%d').date()
        year = SubElement(comicinfo, 'Year')
        year.text = f'{publish_date.year}'

        month = SubElement(comicinfo, 'Month')
        month.text = f'{publish_date.month}'

        month = SubElement(comicinfo, 'Day')
        month.text = f'{publish_date.day}'

    else:
        year = SubElement(comicinfo, 'Year')
        year.text = None

        month = SubElement(comicinfo, 'Month')
        month.text = None

        month = SubElement(comicinfo, 'Day')
        month.text = None

    writer = SubElement(comicinfo, 'Writer')
    writer.text = tryIter(metadata.staff['story'])

    penciller = SubElement(comicinfo, 'Penciller')
    penciller.text = tryIter(metadata.staff['art'])

    inker = SubElement(comicinfo, 'Inker')
    inker.text = tryIter(metadata.staff['art'])

    colorist = SubElement(comicinfo, 'Colorist')
    colorist.text = tryIter(metadata.staff['art'])

    letterer = SubElement(comicinfo, 'Letterer')
    letterer.text = tryIter(metadata.staff['art'])

    cover_artist = SubElement(comicinfo, 'CoverArtist')
    if tryIter(metadata.staff['cover']):
        cover_artist.text = tryIter(metadata.staff['cover'])
    else:
        cover_artist.text = tryIter(metadata.staff['art'])

    publisher = SubElement(comicinfo, 'Publisher')
    publisher.text = tryIter(metadata.serializations)

    genre = SubElement(comicinfo, 'Genre')
    for mg in metadata.genres:
        if genre.text is not None:
            genre.text += f',{mg}'
        else:
            genre.text = f'{mg}'

    web = SubElement(comicinfo, 'Web')
    web.text = tryIter(metadata.url)

    language = SubElement(comicinfo, 'LanguageISO')
    language.text = 'en'

    manga = SubElement(comicinfo, 'Manga')
    manga.text = 'Yes'

    notes = SubElement(comicinfo, 'Notes')
    notes.text = f'Scraped metadata from AniList and MyAnimeList (using Jikan API) on {metadata.scrape_date}'

    comicinfo.set('xmlns:xsd', 'http://www.w3.org/2001/XMLSchema')
    comicinfo.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')

    hentai = False
    if metadata.source == "Fakku" or metadata.source == "NHentai":
        hentai = True

    LOG.info(f'Finished creating ComicInfo object for "{metadata.series_title}", chapter {chapter_number}.',
             extra=logging_info)
    return [parseString(tostring(comicinfo,short_empty_elements=False)).toprettyxml(indent="   "), hentai]


def reconstruct_manga_chapter(comicinfo_xml, manga_file_path, isHentai,logging_info):
    folderdir = "\\".join(str(manga_file_path.absolute()).split("\\")[:-1])
    try:
        with ZipFile(manga_file_path, 'a') as zipfile:
            zipfile.writestr('ComicInfo.xml', comicinfo_xml)
    except Exception as e:
        LOG.exception(e, extra=logging_info)
        LOG.warning('Manga Tagger is unfamiliar with this error. Please log an issue for investigation.',
                    extra=logging_info)
        return
    if isHentai:
        dirh = Path(folderdir.replace("Manga", "Hentai"))
        if not os.path.isdir(dirh):
            os.mkdir(dirh)
        shutil.move(manga_file_path, Path(str(manga_file_path.absolute()).replace("Manga", "Hentai")))
        shutil.rmtree(Path(folderdir))
        folderdir = dirh
    thumb(folderdir)

    LOG.info(f'ComicInfo.xml has been created and appended to "{manga_file_path}".', extra=logging_info)


def tryIter(x):
    if isinstance(x, str):
        return x
    if x is None:
        return "None"
    try:
        return next(iter(x))
    except StopIteration:
        return "None"


def dbSearch(string, mode):
    if mode == 0:
        return MetadataTable.search_by_search_value(string)
    elif mode == 1:
        return MetadataTable.search_by_series_title(string)
    elif mode == 2:
        return MetadataTable.search_by_series_title_eng(string)