import gzip
import shutil
from io import BytesIO
from pathlib import Path
import requests

from tqdm import tqdm


class FilesDownloader:
    class DownloadError(Exception):
        pass

    class ExtractAndSaveError(Exception):
        pass

    @classmethod
    def _extract_file_and_save(cls, temp_directory: Path, response_data: BytesIO, file_name: str) -> bool:
        try:
            extracted_file_name = file_name.rsplit(".", 1)[0]
            file_path = Path(temp_directory, extracted_file_name)
            with gzip.open(response_data, "rb") as compressed_file, \
                 file_path.open("wb") as new_file:
                shutil.copyfileobj(compressed_file, new_file)
            return True
        except Exception as e:
            error_message = f"\033[91mError during extraction or saving of file: {file_name}, error: {e}\033[0m"
            raise cls.ExtractAndSaveError(error_message) from None

    @classmethod
    def download_extract_and_save_files(cls, temp_directory: Path, files_to_download: list) -> bool:

        for url in files_to_download:
            file_name = url.rsplit("/", 1)[1]
            response_status_code = None

            print(f"\n\033[93m{file_name}\033[0m", flush=True)

            try:
                response = requests.get(url, stream=True, timeout=300)
                response_status_code = response.status_code
                if response_status_code == 200:

                    response_data = BytesIO()
                    total_size = int(response.headers.get("content-length", 0))

                    bar_format = "\033[92m{l_bar}{bar:10}{r_bar}{bar:-10b}\033[0m"
                    with tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading",
                              bar_format=bar_format) as pbar:
                        current_downloaded_size = 0
                        for chunk in response.iter_content(chunk_size=1024):
                            if chunk:
                                current_chunk_size = len(chunk)
                                current_downloaded_size += current_chunk_size
                                pbar.update(current_chunk_size)
                                response_data.write(chunk)
                        pbar.clear()
                        pbar.close()

                    response_data.seek(0)
                    print(f"{file_name} downloaded", flush=True)
                    if not cls._extract_file_and_save(temp_directory, response_data, file_name):
                        return False

            except Exception as e:
                error_message = f"\033[91mError during download: {response_status_code}, {e}\033[0m"
                raise cls.DownloadError(error_message) from None

        return True

    @staticmethod
    def delete_files(temp_directory: Path, files_to_delete: list) -> None:
        for url in files_to_delete:
            file_path = Path(temp_directory, url.split("/")[-1].replace(".gz", ""))
            if file_path.exists():
                file_path.unlink()
                print(f"File {file_path} deleted.")
            else:
                print(f"File {file_path} does not exist.")
