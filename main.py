import datetime
from db import DataBase
from scraper import Scraper


def run() -> None:
    database = DataBase()
    database.create_tables()

    scraper = Scraper()
    for movie_data in scraper.scrape_movies_for_date(datetime.date(2025, 3, 14)):
        database.save_movie(movie_data)


if __name__ == "__main__":
    run()
