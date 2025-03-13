import logging
import os
import sqlite3
from typing import Any, Dict, Tuple


logger = logging.getLogger("afisha")


class DataBase:
    """
    Класс для работы с базой данных.
    """

    PATH: str = os.path.join("data", "afisha.db")

    def _check_database_existence(self):
        dir_path = os.path.dirname(self.PATH)
        os.makedirs(dir_path, exist_ok=True)

    @staticmethod
    def _create_cinemas_table(cursor: sqlite3.Cursor) -> None:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Cinemas (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        address TEXT,
        rating REAL)
        """)

    @staticmethod
    def _create_movies_table(cursor: sqlite3.Cursor) -> None:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        production_year INTEGER,
        country TEXT,
        duration TEXT,
        synopsis TEXT,
        rating REAL,
        url TEXT,
        image BLOB)
        """)

    @staticmethod
    def _create_schedule_table(cursor: sqlite3.Cursor) -> None:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id INTEGER,
        cinema_id INTEGER,
        session_id TEXT,
        datetime TEXT,
        min_price REAL,
        FOREIGN KEY(movie_id) REFERENCES Movies(id),
        FOREIGN KEY(cinema_id) REFERENCES Cinemas(id))
        """)

    def _get_connection_and_cursor(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        connection = sqlite3.connect(self.PATH)
        cursor = connection.cursor()
        return connection, cursor

    @staticmethod
    def _save_cinema(connection: sqlite3.Connection, cursor: sqlite3.Cursor, cinema_data: Dict[str, Any]) -> None:
        cursor.execute("SELECT * FROM Cinemas WHERE id = ?", (cinema_data["id"],))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Cinemas (id, name, address, rating) VALUES (?, ?, ?, ?)",
                           (cinema_data["id"], cinema_data["name"], cinema_data["address"], cinema_data["rating"]))
            logger.info("Cinema '%s' is inserted into Cinemas table", cinema_data["name"])
            connection.commit()

    @staticmethod
    def _save_session(connection: sqlite3.Connection, cursor: sqlite3.Cursor, movie_id: int, cinema_id: int,
                      session_data: Dict[str, Any]) -> None:
        cursor.execute("SELECT * FROM Schedule WHERE movie_id = ? AND cinema_id = ? AND session_id = ?",
                       (movie_id, cinema_id, session_data["session_id"]))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Schedule (id, movie_id, cinema_id, session_id, datetime, min_price) VALUES "
                           "(NULL, ?, ?, ?, ?, ?)",
                           (movie_id, cinema_id, session_data["session_id"], session_data["datetime"],
                            session_data["min_price"]))
            logger.info("Session '%s' is inserted into Schedule table", session_data["session_id"])
            connection.commit()

    def create_tables(self) -> None:
        self._check_database_existence()
        connection, cursor = self._get_connection_and_cursor()
        self._create_movies_table(cursor)
        self._create_cinemas_table(cursor)
        self._create_schedule_table(cursor)
        connection.commit()
        connection.close()

    def save_movie(self, movie_data: Dict[str, Any]) -> None:
        """
        :param movie_data: словарь с данными о фильме, которые нужно сохранить.
        """

        connection, cursor = self._get_connection_and_cursor()
        cursor.execute("SELECT * FROM Movies WHERE id = ?", (movie_data["id"],))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Movies (id, name, production_year, country, duration, synopsis, rating, "
                           "url, image) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (movie_data["id"], movie_data["name"], movie_data["production_year"], movie_data["country"],
                            movie_data["duration"], movie_data["synopsis"], movie_data["rating"], movie_data["url"],
                            movie_data["image"]))
            logger.info("Movie '%s' is inserted into Movies table", movie_data["name"])
            connection.commit()
        connection.close()

    def save_sessions(self, movie_id: int, sessions_data: Dict[str, Any]) -> None:
        connection, cursor = self._get_connection_and_cursor()
        self._save_cinema(connection, cursor, sessions_data["place"])
        for session_data in sessions_data["sessions"]:
            self._save_session(connection, cursor, movie_id, sessions_data["place"]["id"], session_data)

        connection.close()
