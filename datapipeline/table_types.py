import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Types(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.basics.tsv - column titleType into Pandas series, filter unique values and create dataframe
            s = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=["titleType"],
                nrows=cls.row_limit_size if cls.debug else None,
            )["titleType"]
            s = s.unique()  # type: ignore
            df = pd.DataFrame({"titleType": s})

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        mapping = {
            "movie": "Movie",
            "short": "Short",
            "tvEpisode": "TV Episode",
            "tvMiniSeries": "TV MiniSeries",
            "tvMovie": "TV Movie",
            "tvPilot": "TV Pilot",
            "tvSeries": "TV Series",
            "tvShort": "TV Short",
            "tvSpecial": "TV Special",
            "video": "Video",
            "videoGame": "Video Game",
        }

        try:

            df = (
                df
                .assign(
                    id=df["titleType"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                )
                .replace(mapping)
                .rename(columns={"titleType": "type"})
                .sort_values(by=["type"])
                .reindex(columns=["id", "type"])
                .reset_index(drop=True)
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
