import os
import sqlite3
from typing import Any, Dict


class DataBase:

    PATH: str = os.path.join("data", "afisha.db")

    def _check_database_existence(self):
        dir_path = os.path.dirname(self.PATH)
        os.makedirs(dir_path, exist_ok=True)

    def _get_connection_and_cursor(self):
        connection = sqlite3.connect(self.PATH)
        cursor = connection.cursor()
        return connection, cursor

    def create_tables(self) -> None:
        self._check_database_existence()
        connection, cursor = self._get_connection_and_cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        production_year INTEGER,
        country TEXT,
        duration TEXT,
        synopsis TEXT,
        rating REAL,
        image BLOB)
        """)

        connection.commit()
        connection.close()

    def save_movie(self, movie_data: Dict[str, Any]) -> None:
        connection, cursor = self._get_connection_and_cursor()

        cursor.execute("SELECT * FROM Movies WHERE name = ?", (movie_data["name"],))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Movies (id, name, production_year, country, duration, synopsis, rating, image)"
                           " VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)",
                           (movie_data["name"], movie_data["production_year"], movie_data["country"],
                            movie_data["duration"], movie_data["synopsis"], movie_data["rating"], movie_data["image"]))
