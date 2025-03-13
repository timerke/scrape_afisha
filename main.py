import datetime
from common import logger
from db import DataBase
from scraper import Scraper


def run() -> None:
    logger.set_logger()

    database = DataBase()
    database.create_tables()

    scraper = Scraper()
    for movie_data in scraper.scrape_movies():
        database.save_movie(movie_data)
        date = datetime.date(2025, 3, 15)
        for sessions_data in scraper.scrape_schedule_for_movie(movie_data["url"], date):
            database.save_sessions(movie_data["id"], sessions_data)


if __name__ == "__main__":
    run()
