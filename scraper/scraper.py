import datetime
import logging
import re
from typing import Any, Dict, Generator, List, Optional
import requests


logger = logging.getLogger("afisha")


class Scraper:
    """
    Класс для сбора данных о фильмах с сайте www.afisha.ru.
    """

    PAGE_SIZE: int = 20

    @staticmethod
    def _get_cinema_info(place_json: Dict[str, Any]) -> Dict[str, Any]:
        return {"id": place_json["ID"],
                "name": place_json["Name"],
                "address": place_json["Address"],
                "rating": place_json["Rating"]}

    def _get_movie_data_from_page(self, page_json: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """
        :param page_json: JSON о фильмах на странице.
        :return: словари с данными о фильмах, которые можно сохранить в базу данных.
        """

        movie_items = page_json["ScheduleWidget"]["Items"]
        for movie_item in movie_items:
            yield {"image": self._get_movie_poster(movie_item),
                   **self._get_movie_info(movie_item)}

    @staticmethod
    def _get_movie_info(movie_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        :param movie_json: JSON о фильме с сайта.
        :return: словарь с информацией о фильме.
        """

        return {"id": movie_json["ID"],
                "name": movie_json["Name"],
                "production_year": movie_json["ProductionYear"],
                "country": movie_json["Country"],
                "duration": movie_json["Duration"],
                "synopsis": movie_json["Synopsis"],
                "rating": movie_json["Rating"],
                "url": movie_json["Url"]}

    @staticmethod
    def _get_movie_part_for_schedule_url(movie_url: str) -> str:
        result = re.match(r"/movie/(?P<movie_part>.*)/.*", movie_url)
        return result.group("movie_part")

    @staticmethod
    def _get_movie_poster(movie_json: Dict[str, Any]) -> Optional[bytes]:
        """
        :param movie_json: JSON о фильме с сайта.
        :return: изображение фильма.
        """

        poster_url = movie_json["Image1x1"]["Url"]
        result = requests.get(poster_url)
        return result.content if result.status_code == 200 else None

    def _get_schedule_data_from_page(self, start_date: datetime.date, end_date: datetime.date,
                                     page_json: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        schedule_items = page_json["ScheduleWidget"]["ScheduleList"]["Items"]
        for schedule_item in schedule_items:
            try:
                sessions_info = self._get_sessions_info(start_date, end_date, schedule_item["Sessions"])
            except StopIteration:
                return

            yield {"place": self._get_cinema_info(schedule_item["Place"]),
                   "sessions": sessions_info}

    @staticmethod
    def _get_sessions_info(start_date: datetime.date, end_date: datetime.date, sessions: List[Dict[str, Any]]
                           ) -> List[Dict[str, Any]]:
        info = []
        for session_item in sessions:
            date_time = datetime.datetime.strptime(session_item["DateTime"], "%Y-%m-%dT%H:%M:%S")
            if date_time.date() < start_date or date_time.date() > end_date:
                raise StopIteration

            if session_item["SourceSessionID"] and session_item["MinPriceFormatted"]:
                info.append({"datetime": date_time,
                             "min_price": session_item["MinPriceFormatted"],
                             "session_id": session_item["SourceSessionID"]})

        return info

    def _get_url_for_movies(self, page: int, date: Optional[datetime.date] = None) -> str:
        """
        :param page: номер страницы с фильмами;
        :param date: дата, для которой получить фильмы.
        :return: URL, по которому можно получить страницу с фильмами.
        """

        if date:
            date = date.strftime("%Y-%m-%d")
            return (f"https://www.afisha.ru/msk/schedule_cinema/?sort=recommendations&date={date}--{date}&"
                    f"page={page}&pageSize={self.PAGE_SIZE}")

        return f"https://www.afisha.ru/msk/schedule_cinema/?sort=recommendations&page={page}&pageSize={self.PAGE_SIZE}"

    def _get_url_for_schedule(self, page: int, movie_url: str, start_date: datetime.date, end_date: datetime.date
                              ) -> str:
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        movie_part = self._get_movie_part_for_schedule_url(movie_url)
        return (f"https://www.afisha.ru/msk/schedule_cinema_product/{movie_part}/?view=list&sort=rating&date="
                f"{start_date}--{end_date}&page={page}&pageSize={self.PAGE_SIZE}")

    def scrape_movies(self, date: datetime.date = None) -> Generator[Dict[str, Any], None, None]:
        """
        :param date: дата, для которой получить фильмы.
        :return: словари с данными о фильмах, которые можно сохранить в базу данных.
        """

        page = 1
        movies_on_page = True
        while movies_on_page:
            logger.info("Scrape page #%d with movies", page)
            url = self._get_url_for_movies(page, date)
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

    def scrape_schedule_for_movie(self, movie_url: str, start_date: datetime.date,
                                  end_date: Optional[datetime.date] = None) -> Generator[Dict[str, Any], None, None]:
        end_date = end_date or start_date
        page = 1
        schedules_on_page = True
        while schedules_on_page:
            logger.info("Scrape page #%d with schedule", page)
            url = self._get_url_for_schedule(page, movie_url, start_date, end_date)
            response = requests.get(url, headers={"Accept": "application/json",
                                                  "Connection": "keep - alive",
                                                  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
                                                                "Gecko/20100101 Firefox/136.0"})
            schedules_on_page = False
            if response.status_code == 200:
                for schedule_data in self._get_schedule_data_from_page(start_date, end_date, response.json()):
                    schedules_on_page = True
                    yield schedule_data

            page += 1
