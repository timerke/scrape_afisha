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


if __name__ == "__main__":
    run()
