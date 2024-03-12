import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Titles(AbstractTable):
    csv = None

    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.basics.tsv as dataframe
            cls.csv = pd.read_csv(Path(cls.temp_directory, file_name), sep="\t", low_memory=False,
                                  nrows=cls.row_limit_size if cls.debug else None)
            title_basics = cls.csv

            # read title.ratings.tsv as dataframe
            title_ratings = pd.read_csv(
                Path(cls.temp_directory, second_file_name),  # type: ignore
                sep="\t",
                low_memory=False,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            # left join
            df = pd.merge(title_basics, title_ratings, on="tconst", how="left")  # noqa: PD015
            del title_basics, title_ratings

            df = (
                df
                .drop(columns=["originalTitle"])
                .replace({"\\N": np.nan})
                .assign(
                    runtime_minutes=pd.to_numeric(df["runtimeMinutes"], errors="coerce").astype("Int64"),
                    type_id=df["titleType"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                    genre_id=df["genres"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                )
                .drop(columns=["runtimeMinutes", "titleType", "genres"], axis=1)
                .reset_index()
                .rename(columns={
                    "tconst": "id",
                    "primaryTitle": "name",
                    "isAdult": "is_adult",
                    "startYear": "start_year",
                    "endYear": "end_year",
                    "runtimeMinutes": "runtime_minutes",
                    "averageRating": "average_rating",
                    "numVotes": "number_of_votes",
                })
                .reindex(
                    columns=[
                        "id",
                        "name",
                        "type_id",
                        "genre_id",
                        "is_adult",
                        "start_year",
                        "end_year",
                        "average_rating",
                        "number_of_votes",
                        "runtime_minutes",
                    ])
                .sort_values(by=["name"])
                .drop_duplicates(subset=["id"])
                .reset_index(drop=True)
                .astype({
                    "id": str,
                    "name": str,
                    "type_id": str,
                    "genre_id": str,
                    "is_adult": bool,
                    "start_year": pd.Int64Dtype(),
                    "end_year": pd.Int64Dtype(),
                    "average_rating": float,
                    "number_of_votes": pd.Int64Dtype(),
                    "runtime_minutes": pd.Int64Dtype(),
                })
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
