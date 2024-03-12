import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class TitlesGenres(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.basics.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                low_memory=False,
                usecols=["tconst", "genres"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .assign(
                    genre=df["genres"].str.split(","),
                )
                .explode("genre")
                .drop("genres", axis=1)
                .dropna(subset=["genre"])
            )

            df = (
                df
                .assign(
                    genre_id=df["genre"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                )
                .drop("genre", axis=1)
                .rename(columns={"tconst": "id", "genre": "genre_id"})
                .drop_duplicates(subset=["id", "genre_id"])
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
