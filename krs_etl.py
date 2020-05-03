import dataclasses
import math
import logging
import gc
import threading
import time
from enum import Enum
import requests

from dacite import from_dict
from google.cloud import bigquery

from src.config import BaseConfig
from src.models.rejestrio_record import RejestrIoRecord

KRS_ENDPOINT = BaseConfig.REJESTR_IO_BASE_URL + '/krs'
HEADERS = {'Authorization': BaseConfig.REJESTR_IO_KEY}
MAXIMUM_BIGQUERY_BATCH_SIZE = 500
ITEMS_PER_PAGE = 100
MAX_PAGE = 1000
FAILED_PAGES_THRESHOLD = 10

FAILED_PAGES = []

logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s',
                    level=logging.INFO)
LOGGER = logging.getLogger("Rejestr.io ETL")


class Sort(Enum):
    ASCENDING = "first_entry_date:asc"
    DESCENDING = "first_entry_date:desc"


def normalize(items):
    """Normalizes data to a unified structure before inserting to BigQuery."""
    LOGGER.info("Starting normalization")
    start_normalize = time.time()
    normalized_items = []
    for item in items:
        normalized_items.append(dataclasses.asdict(from_dict(data_class=RejestrIoRecord,
                                                             data=item)))
    log_execution_time(start_normalize, "Normalization took: %s")
    return normalized_items


class KrsEtl:
    def __init__(self, start_date, project_id, dataset_id):
        self.semaphore = threading.Lock()
        self.client = bigquery.Client(project_id)
        self.dataset_id = dataset_id
        self.current_date = start_date
        self.current_page = 1
        self.processed = 0
        self.current_throttle = 0

    def run(self):
        """Processes batches until there is no more data to process."""
        while self.process_batch() is True:
            LOGGER.info("Batch processed")
            gc.collect()

    def process_batch(self):
        """Creates batch by getting records from rejestrio until MAXIMUM_BIGQUERY_BATCH_SIZE is
        reached, normalizes and inserts batch to big query."""
        items_in_batch = []
        while len(items_in_batch) < MAXIMUM_BIGQUERY_BATCH_SIZE:
            records = self.get_krs_records()
            if records is None:
                LOGGER.info("No more KRS records. Exiting...")
                return False
            LOGGER.info("Extending items in batch (len:%s) with %s records from Rejestr.io",
                        len(items_in_batch),
                        len(records))
            items_in_batch.extend(records)
            if len(records) < ITEMS_PER_PAGE:
                LOGGER.info("Rejestr.io returned %s records. "
                            "Committing batch.",
                            len(records))
                break
        normalized = normalize(items_in_batch)
        self.insert_to_bigquery(normalized)
        LOGGER.info("Batch committed. Processed: %s records",
                    self.processed)
        return True

    def get_krs_records(self):
        """Gets krs records from rejestr.io. Only one thread can use this method at the same time,
        semaphore included. Dynamic throttle modification included as rejestr.io api limits are
        not known. Will retry until number of failed retries reaches FAILED_PAGES_THRESHOLD."""
        self.semaphore.acquire()
        LOGGER.info("Throttling: %s",
                    self.current_throttle)
        time.sleep(self.current_throttle)
        LOGGER.info("Getting data for date '%s' page '%s'",
                    self.current_date,
                    self.current_page)
        rejestrio_response_time = time.time()
        krs_search_params = {
            'sort': Sort.ASCENDING.value,
            'page': self.current_page,
            'per_page': ITEMS_PER_PAGE,
        }
        retry = 0
        while retry < FAILED_PAGES_THRESHOLD:
            response = requests.get(url=KRS_ENDPOINT +
                                    "?first_entry_date=gte:{}".format(self.current_date),
                                    headers=HEADERS,
                                    params=krs_search_params).json()
            if 'code' in response:
                LOGGER.error("Error: %s. Retry for current date '%s' page '%s'",
                             response['message'],
                             self.current_date,
                             self.current_page)
                retry += 1
                self.current_throttle += 1
                LOGGER.info("Throttling: %s",
                            self.current_throttle)
                time.sleep(self.current_throttle)
                continue
            break
        if retry == FAILED_PAGES_THRESHOLD:
            failed = {'date': self.current_date,
                      'page': self.current_page,
                      'code': response['code'],
                      'message': response['message']}
            LOGGER.info("Sth went wrong wile collecting page %s",
                        failed)
            FAILED_PAGES.append(failed)
            LOGGER.error("More than %s failed pages. Exiting...",
                         FAILED_PAGES_THRESHOLD)
            LOGGER.error("Failed pages: %s",
                         FAILED_PAGES)
            raise Exception('Job reached failed pages threshold')
        self.current_throttle = math.floor(self.current_throttle / 2)
        LOGGER.info("Request successful. Reducing throttle: %s",
                    self.current_throttle)
        LOGGER.info("Got data for date '%s' page '%s'",
                    self.current_date,
                    self.current_page)
        log_execution_time(rejestrio_response_time,
                           "Rejestrio responded in: %s")
        items = response['items']
        self.current_page += 1
        if self.current_page == MAX_PAGE:
            self.current_page = 1
            new_date = items[len(items) - 1]['first_entry_date']
            LOGGER.info("Run out of pages for date '%s' changing to '%s'",
                        self.current_date,
                        new_date)
            self.current_date = new_date
        if len(items) == 0:
            self.semaphore.release()
            return None
        self.processed += len(items)
        self.semaphore.release()
        return items

    def insert_to_bigquery(self, items_in_batch):
        """Inserts current batch to BQ and logs errors if any."""
        insert_start_time = time.time()
        LOGGER.info("Sending %s documents to bigquery",
                    len(items_in_batch))
        table = self.client.get_table(self.dataset_id)
        errors = self.client.insert_rows(table, items_in_batch)
        if len(errors) > 0:
            LOGGER.error("Exception while committing files to bigquery. First item in batch: %s",
                         items_in_batch[0])
            raise Exception(errors)
        log_execution_time(insert_start_time, "Insert rows to BigQuery took: %s")


def log_execution_time(start_time, message="Elapsed : %s"):
    """Logs execution time."""
    execution_time = time.time() - start_time
    LOGGER.info(message, round(execution_time, 2))


def log_message(message):
    """Logs messages."""
    LOGGER.info(message)
