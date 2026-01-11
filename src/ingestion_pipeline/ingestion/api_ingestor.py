import json
import logging
import time
from requests import Request, Session
from typing import Optional, Dict, Any, List, Iterable 
from datetime import datetime,date,timezone 
from pathlib import Path
import requests


from ingestion_pipeline.config.settings import DEFAULT_PARAMS



class TheNewsApiClient:
    def __init__(self,
                 base_url: str,
                 max_retries: int = 5,
                 backoff_factor: float = 0.5):

        """
        Initialize the API ingestor.

        Args:
            base_url (str): Base URL of the API.
            max_retries (int): Number of retries for failed requests.
            backoff_factor (float): Backoff multiplier for retries.
        """

        self.base_url = base_url
        self.session = requests.Session()
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor


        self.session.headers.update(DEFAULT_PARAMS)


    def _get_all_posts(self, url: str, params: Dict[str, Any]):

        """
        Make a GET request with retry logic.

        Args:
            url (str): Full API endpoint URL.
            params (dict, optional): Query parameters.

        Returns:
            dict: Parsed JSON response.
        """

        for retries in range (1, self.max_retries +1):
              try:
                  logging.info(f"Fetching page... URL={url}")
                  response = self.session.get(url, params=params, timeout=60)

                  response.raise_for_status()
                  return response.json()

              except requests.exceptions.HTTPError as e:

                  status = e.response.status_code

                  if status < 500 and status != 429:
                        logging.error(f"Permanent HTTP error: {status}")
                        raise  # Fail immediately for 400, 401, 404, etc.


                  if retries == self.max_retries:
                        logging.error(f"Max retries reached for status {status}")
                        raise  # Preserves the original HTTPError for debugging

                  # Retry other HTTP errors with exponential backoff
                  sleep_time = self.backoff_factor * (2 ** (retries - 1))
                  logging.warning(f"Retrying after {sleep_time}s...")

                  time.sleep(sleep_time)


              except Exception as e:
                  logging.error(f"Unexpected error: {e}")

                  if retries == self.max_retries:
                        logging.exception(f"Max retries reached for status {e}")
                        raise

                  sleep_time = self.backoff_factor * (2 ** (retries - 1))
                  time.sleep(sleep_time)
              

    def fetch_paginated(
        self,
        endpoint: str,
        params: Dict[str, Any],
        page_param: str = "page",
        per_page_param: str = "limit",
        max_pages: Optional[int] = None
    ) -> Iterable[dict]:
        
        """
        Generator over articles from TheNewsAPI endpoints that return:
          - data: [...]
        Pagination uses `page` + `limit`
        """
        

        current_page = 1 # Start with a simple page number 1

        while True:
            # The API limit is 3 per page, per docs
            limit = 3


            paged_params = params.copy() if params else {}
            paged_params.update({page_param: current_page, per_page_param: limit})

            logging.info(f"Fetching page {current_page} from {self.base_url}{endpoint}")

            url = f"{self.base_url}{endpoint}"
            data = self._get_all_posts(url=url, params=paged_params)


            items = data.get("data", []) # type: ignore

            # --- Break Conditions ---
            if not items:
                break

            for item in items:
                yield item


            if len(items) < limit:
                logging.info("Last available page reached.")
                break

            if max_pages and current_page >= max_pages:
                break

            # --- Next Iteration ---
            current_page += 1
            time.sleep(2)

    @staticmethod
    def batch(iterable: Iterable[Any], batch_size: int = 50) -> Iterable[list[Any]]:

        batch: list[Any] = []

        for item in iterable:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch