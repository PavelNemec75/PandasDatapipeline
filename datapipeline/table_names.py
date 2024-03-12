import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Names(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.basics.tsv as dataframe - only column tconst
            title_basics = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                low_memory=False,
                usecols=["tconst"],
            )

            # read title.akas.tsv as dataframe
            title_akas = pd.read_csv(
                Path(cls.temp_directory, second_file_name),  # type: ignore
                sep="\t", low_memory=False,
                usecols=["titleId", "ordering", "title", "region", "language", "isOriginalTitle"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        # inner join
        df = pd.merge(title_basics, title_akas, left_on="tconst", right_on="titleId", how="inner")  # noqa: PD015
        del title_basics, title_akas

        try:

            df = (
                df
                .replace({"\\N": np.nan})
                .drop("titleId", axis=1)
                .rename(columns={"tconst": "id", "title": "name", "isOriginalTitle": "is_original_title"})
                .sort_values(by=["name"])
                .dropna(subset=["id"], how="all")
                .drop_duplicates(subset=["id", "ordering"])
                .reset_index(drop=True)
                .astype({
                    "id": str,
                    "ordering": int,
                    "name": str,
                    "region": str,
                    "language": str,
                    "is_original_title": bool,
                })
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
