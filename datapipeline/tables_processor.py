import time
from datetime import datetime
from pathlib import Path
from typing import TypeVar

from .abstract_table import AbstractTable
from .table_titles import Titles
from .table_types import Types
from .table_episodes import Episodes
from .table_names import Names
from .table_titles_genres import TitlesGenres
from .table_genres import Genres
from .table_principals import Principals
from .table_principals_characters import PrincipalsCharacters
from .table_characters import Characters
from .table_persons import Persons
from .table_persons_professions import PersonsProfessions
from .table_professions import Professions
from .table_categories import Categories
from .table_jobs import Jobs


class TablesProcessor:
    T = TypeVar("T")

    class TablesProcessorError(Exception):
        pass

    @staticmethod
    def _set_variables(temp_directory: Path, row_limit_size: int, batch_size: int, debug: bool) -> None:

        AbstractTable.temp_directory = temp_directory
        AbstractTable.row_limit_size = row_limit_size
        AbstractTable.batch_size = batch_size
        AbstractTable.debug = debug

    @classmethod
    def process_tables(cls, temp_directory: Path, row_limit_size: int, batch_size: int, debug: bool) -> None:

        cls._set_variables(temp_directory, row_limit_size, batch_size, debug)
        start_time = time.time()
        print(f"\n\033[92mStarting : {datetime.now().strftime('%H:%M:%S')}\033[0m")  # noqa: DTZ005

        try:

            Titles.process_table("title.basics.tsv", "title.ratings.tsv")
            Types.process_table("title.basics.tsv", None)
            Episodes.process_table("title.episode.tsv", "title.basics.tsv")
            Names.process_table("title.basics.tsv", "title.akas.tsv")
            TitlesGenres.process_table("title.basics.tsv", None)
            Genres.process_table("title.basics.tsv", None)
            Principals.process_table("title.principals.tsv", "title.basics.tsv")
            PrincipalsCharacters.process_table("title.principals.tsv", "title.basics.tsv")
            Characters.process_table("title.principals.tsv", None)
            Persons.process_table("name.basics.tsv", None)
            PersonsProfessions.process_table("name.basics.tsv", None)
            Professions.process_table("name.basics.tsv", None)
            Categories.process_table("title.principals.tsv", None)
            Jobs.process_table("title.principals.tsv", None)

        except Exception as e:
            error_message = f"Error during tables processing: {e}"
            raise cls.TablesProcessorError(error_message) from None

        end_time = time.time()
        print(f"\n\033[92mTotal time taken: {end_time - start_time} seconds\033[0m")
