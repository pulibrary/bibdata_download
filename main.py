# Standard library imports
from logging import Formatter
from logging import INFO
from logging import StreamHandler
from logging import getLogger
from os import getcwd
from os import listdir
from os import makedirs
from os import remove
from os import rename
from os.path import dirname
from os.path import join
from tarfile import open as open_tarfile
from urllib.parse import unquote_plus

# Third party imports
from requests import Response
from requests import get

logger = getLogger(__name__)


def set_up_logging() -> None:
    root_logger = getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    log_level = INFO
    formatter_str = "%(asctime)s - %(levelname)s - %(message)s"
    console_handler = StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(Formatter(formatter_str))
    root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level)


def get_filename_from_content_disposition(response: Response, num: str) -> str:
    content_disposition = response.headers.get("Content-Disposition")
    file_name = f"{num}_{unquote_plus(content_disposition.split(";")[1].split('"')[1])}"  # FRAGILE
    return file_name


def download_file(url: str, directory_path: str) -> str:
    with get(url, stream=True) as resp:
        resp.raise_for_status()
        num = url.split("/")[-1]
        file_name = get_filename_from_content_disposition(resp, num)
        full_path = join(directory_path, file_name)
        with open(full_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {file_name} to {directory_path}")
    return full_path


def download_bd_files(
    set_url: str, directory_path: str, start: int = 1, limit: int | None = None
) -> list[str]:
    if directory_path.startswith("/"):
        makedirs(directory_path, exist_ok=True)
    else:
        directory_path = join(getcwd(), directory_path)
        makedirs(directory_path)
    index = get(set_url).json()
    file_urls = [d["dump_file"] for d in index["files"]["bib_records"]]
    files = []
    for index, file_url in enumerate(file_urls[start - 1 : start + limit - 1], start=1):
        logger.info(f"Downloading file {index}/{len(file_urls)}")
        files.append(download_file(file_url, directory_path))
    return files


def untar(file_path: str) -> None:
    dir = dirname(file_path)
    with open_tarfile(file_path, "r:gz") as tar:
        tar.extractall(dir)
    remove(file_path)
    logger.info(f"Extracted {file_path} to {dir}")


def tidy_names(dir_path: str) -> list[str]:
    names = []
    for old_name in filter(lambda n: not (n.endswith(".marcxml")), listdir(dir_path)):
        base = old_name.split("[")[0]
        num = old_name.split("_")[-1].zfill(3)
        file_name = f"{base}_{num}.marcxml"
        old_name = join(dir_path, old_name)
        new_name = join(dir_path, file_name)
        rename(old_name, new_name)
        logger.info(f"Renamed {old_name} to {file_name}")
        names.append(new_name)
    return names


if __name__ == "__main__":
    set_up_logging()
    report_url = "https://bibdata.princeton.edu/dumps/12645.json"
    out_dir = "/tmp/full_dump"
    # Twenty at a time
    files = download_bd_files(report_url, out_dir, start=1, limit=20)
    [untar(file) for file in files]
    tidy_names(out_dir)
