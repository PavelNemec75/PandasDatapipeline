import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class PersonsProfessions(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read name.basics.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=["nconst", "primaryProfession"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .replace({"\\N": np.nan})
                .rename(columns={"nconst": "id", "primaryProfession": "professions"})
                .assign(
                    profession=lambda x: x["professions"].str.split(","),
                )
                .explode("profession")
                .drop("professions", axis=1)
                .drop_duplicates(subset=["id", "profession"])
                .dropna(subset=["profession"], how="all")
                .assign(
                    profession_id=lambda x: x["profession"].apply(lambda x: cls._generate_id(x)).astype(str),
                )
                .drop("profession", axis=1)
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
