import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Professions(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read name.basics.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=["primaryProfession"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .replace({"\\N": np.nan})
                .assign(
                    profession=lambda x: x["primaryProfession"].str.split(","),
                )
                .explode("profession")
                .drop("primaryProfession", axis=1)
                .drop_duplicates(subset=["profession"])
                .dropna(subset=["profession"], how="all")
                .assign(
                    id=lambda x: x["profession"].apply(lambda x: cls._generate_id(x)).astype(str),
                    profession=lambda x: x["profession"].str.title().str.replace("_", " "),
                )
                .reindex(columns=["id", "profession"])
                .sort_values(by=["profession"])
                .reset_index(drop=True)
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
