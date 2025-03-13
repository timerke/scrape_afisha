import datetime
import logging
from typing import Any, Dict, Generator, Optional
import requests


logger = logging.getLogger("afisha")


class Scraper:

    def _get_movie_data_from_page(self, page_json: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        movie_items = page_json["ScheduleWidget"]["Items"]
        for movie_item in movie_items:
            movie_data = {"image": self._get_movie_poster(movie_item),
                          **self._get_movie_info(movie_item)}
            yield movie_data

    @staticmethod
    def _get_movie_info(movie_json: Dict[str, Any]) -> Dict[str, Any]:
        return {"id": movie_json["ID"],
                "name": movie_json["Name"],
                "production_year": movie_json["ProductionYear"],
                "country": movie_json["Country"],
                "duration": movie_json["Duration"],
                "synopsis": movie_json["Synopsis"],
                "rating": movie_json["Rating"],
                "url": movie_json["Url"]}

    @staticmethod
    def _get_movie_poster(movie_json: Dict[str, Any]) -> Optional[bytes]:
        poster_url = movie_json["Image1x1"]["Url"]
        result = requests.get(poster_url)
        return result.content if result.status_code == 200 else None

    @staticmethod
    def _get_url(page: int, date: Optional[datetime.date] = None) -> str:
        if date:
            date = date.strftime("%Y-%m-%d")
            return (f"https://www.afisha.ru/msk/schedule_cinema/?sort=recommendations&date={date}--{date}&"
                    f"page={page}&pageSize=24")

        return f"https://www.afisha.ru/msk/schedule_cinema/?sort=recommendations&page={page}&pageSize=24"

    def scrape_movies(self, date: datetime.date = None) -> Generator[Dict[str, Any], None, None]:
        page = 1
        movies_on_page = True
        while movies_on_page:
            logger.info("Scrape page #%d with movies", page)
            url = self._get_url(page, date)
            response = requests.get(url, headers={"Accept": "application/json",
                                                  "Connection": "keep - alive",
                                                  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
                                                                "Gecko/20100101 Firefox/136.0"})
            movies_on_page = False
            if response.status_code == 200:
                for movie_data in self._get_movie_data_from_page(response.json()):
                    movies_on_page = True
                    yield movie_data

            page += 1
