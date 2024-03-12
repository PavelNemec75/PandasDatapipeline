import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Persons(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read name.basics.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=["nconst", "primaryName", "birthYear", "deathYear"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .replace({"\\N": np.nan})
                .rename(columns={"nconst": "id", "primaryName": "full_name", "birthYear": "birth_year",
                                 "deathYear": "death_year"})
                .drop_duplicates(subset=["id", "full_name", "birth_year", "death_year"])
                .astype({"id": str, "full_name": str, "birth_year": pd.Int64Dtype(), "death_year": pd.Int64Dtype()})
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
