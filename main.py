from pathlib import Path

from datapipeline.files_downloader import FilesDownloader
from datapipeline.tables_processor import TablesProcessor


def main() -> None:

    temp_directory: Path = Path(".data")
    base_url: str = "https://datasets.imdbws.com"
    files_to_download: list = [
        f"{base_url}/title.ratings.tsv.gz",
        f"{base_url}/title.episode.tsv.gz",
        f"{base_url}/name.basics.tsv.gz",
        f"{base_url}/title.basics.tsv.gz",
        f"{base_url}/title.akas.tsv.gz",
        f"{base_url}/title.principals.tsv.gz",
    ]

    """debugging"""
    debug: bool = True
    row_limit_size: int = 1_000_000
    batch_size: int = 100_000

    """production"""
    # debug: bool = False
    # row_limit_size: int = 0
    # batch_size: int = 10_000_000

    try:

        # download, extract and save files
        FilesDownloader.download_extract_and_save_files(temp_directory, files_to_download)
        print("\nFiles downloaded and saved successfully.\n\n")

        # create parquet files from tables
        TablesProcessor.process_tables(temp_directory, row_limit_size, batch_size, debug)
        print("\nTables processed successfully.\n\n")

        # delete temporary files
        FilesDownloader.delete_files(temp_directory, files_to_download)

    except (FilesDownloader.DownloadError, TablesProcessor.TablesProcessorError) as e:
        print(f"Error(s) occurred: {e}")


if __name__ == "__main__":
    main()
