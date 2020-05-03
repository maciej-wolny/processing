import dataclasses
import gc
import threading
from datetime import datetime, timedelta
import time
import logging

from dacite import from_dict
from google.cloud import bigquery
from zeep import Client, Settings
import xmltodict
from enum import Enum

from src.config import BaseConfig
from src.models.ceidg_record import InformacjaOWpisie

OPTIMAL_BIGQUERY_BATCH_SIZE = 500

PROJECT_ID = "simplify-docs"
CEIDG_COLLECTION = "ceidg-prod"
CEIDG_PROCESSED = "processed-ceidg-prod"
MAXIMUM_FIRESTORE_BATCH_SIZE = 500
DAYS_IN_BATCH = 2

total_documents_count = 0
semaphore = threading.Semaphore(3)


class Status(Enum):
    NOT_PROCESSED = "not processed"
    PROCESSING = "processing"
    PROCESSED = "processed"


logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s',
                    level=logging.INFO)
FH = logging.FileHandler('output.log')
LOGGER = logging.getLogger("Rejestr.io ETL")
LOGGER.addHandler(FH)


def log_execution_time(start_time, message="Elapsed : %s"):
    """Logs execution time."""
    execution_time = time.time() - start_time
    LOGGER.info(message, round(execution_time, 2))


def normalize(records):
    normalized = []
    normalization_time = time.time()
    logging.info("Started normalization of %s records", len(records))
    for record in records:
        convert_to_list('Zakazy', 'InformacjaOZakazie', record)
        convert_to_list('SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca', 'InformacjeOSpolce', record)
        convert_to_list('InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego', 'Informacja', record)
        convert_to_list('Uprawnienia', 'Uprawnienie', record)
        convert_to_list('DaneAdresowe', 'AdresyDodatkowychMiejscWykonywaniaDzialalnosci', record)
        normalized.append(dataclasses.asdict(from_dict(data_class=InformacjaOWpisie, data=record)))
    log_execution_time(normalization_time, "Normalize took %s")
    return normalized


def convert_to_list(field_name, nested_field_name, record):
    if field_name == 'DaneAdresowe':
        if nested_field_name in record[field_name] and record[field_name][nested_field_name] is not None and \
                isinstance(record[field_name][nested_field_name]['Adres'], dict):
            record[field_name][nested_field_name]['Adres'] = [
                record[field_name][nested_field_name]['Adres']]
        return record
    if should_be_converted(record, field_name, nested_field_name):
        record[field_name][nested_field_name] = [record[field_name][nested_field_name]]
    return record


def should_be_converted(record, field_name, nested_field_name):
    return field_name in record and record[field_name] is not None and len(record[field_name]) == 1 and (
            isinstance(record[field_name][nested_field_name],
                       dict) or isinstance(record[field_name][nested_field_name], str))


def parse(records):
    logging.info("Parsing %s records...", len(records))
    parsing_time = time.time()
    parsed_result = xmltodict.parse(records)['WynikWyszukiwania']['InformacjaOWpisie']
    log_execution_time(parsing_time, "Parse took %s")
    return parsed_result


class CeidgEtl:
    def __init__(self, start_date, project_id, dataset_id):
        self.semaphore = threading.Semaphore(3)
        self.lock = threading.Lock()
        self.client = bigquery.Client(project_id)
        self.dataset_id = dataset_id
        self.current_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.current_page = 1
        self.processed = 0
        self.current_throttle = 0
        self.url = BaseConfig.API_CEIDG_URL
        settings = Settings(strict=False, xml_huge_tree=True)
        self.soap_client = Client(self.url, settings=settings)

    def run(self):
        """Processes batches until there is no more data to process."""
        while self.process_batch() is True:
            LOGGER.info("Batch processed")
            gc.collect()

    def process_batch(self):
        """Creates batch by getting records from rejestrio until MAXIMUM_BIGQUERY_BATCH_SIZE is
        reached, normalizes and inserts batch to big query."""
        # 1. Sprawdz czy zwraca none
        batch_time = time.time()
        records = self.get_ceidg_records()
        if records is None:
            LOGGER.info("No more CEIDG records. Exiting...")
            return False
        normalized = normalize(records)
        self.insert_to_bigquery(normalized)
        LOGGER.info("Batch committed. Processed: %s records", len(records))
        self.lock.acquire()
        self.processed += len(records)
        self.lock.release()
        log_execution_time(batch_time, "Batch committed in %s")
        logging.info("Processed: %s", self.processed)
        return True

    def get_ceidg_records(self):
        """Gets krs records from rejestr.io. Only one thread can use this method at the same time,
        semaphore included. Dynamic throttle modification included as rejestr.io api limits are
        not known. Will retry until number of failed retries reaches FAILED_PAGES_THRESHOLD."""
        self.lock.acquire()
        start_date = self.current_date
        # if start_date.strftime('%Y-%m-%d') == "1990-04-02":
        #     self.semaphore.release()
        #     raise Exception("End, processed %s", self.processed)
        end_date = start_date + timedelta(days=DAYS_IN_BATCH - 1)
        self.current_date = end_date + timedelta(days=1)
        self.lock.release()
        self.semaphore.acquire()
        logging.info("Starting to process batch [{} - {}]".format(start_date.strftime('%Y-%m-%d'),
                                                                  end_date.strftime('%Y-%m-%d')))
        download_time = time.time()
        request_data = {'AuthToken': BaseConfig.API_CEIDG_TOKEN,
                        'DateFrom': start_date.strftime('%Y-%m-%d'),
                        'DateTo': end_date.strftime('%Y-%m-%d')}
        try:
            ceidg_results = self.soap_client.service.GetMigrationData201901(**request_data)
        except ConnectionError:
            try:
                ceidg_results = self.soap_client.service.GetMigrationData201901(**request_data)
            except ConnectionError:
                self.semaphore.release()
                raise ConnectionError(
                    "batch [{} - {}]".format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        log_execution_time(download_time, "Download took %s")
        parsed_time = time.time()
        parsed_results = xmltodict.parse(ceidg_results)['WynikWyszukiwania']['InformacjaOWpisie']
        logging.info("Downloaded {} ceidg records".format(len(parsed_results)))
        log_execution_time(parsed_time, "Parse took %s")
        self.semaphore.release()
        return parsed_results

    def insert_to_bigquery(self, items_in_batch):
        """Inserts current batch to BQ and logs errors if any."""
        insert_start_time = time.time()
        optimal_size_batches = [items_in_batch[i:i + OPTIMAL_BIGQUERY_BATCH_SIZE] for i in
                                range(0, len(items_in_batch), OPTIMAL_BIGQUERY_BATCH_SIZE)]
        LOGGER.info("Sending %s documents to bigquery",
                    len(items_in_batch))
        for batch in optimal_size_batches:
            table = self.client.get_table(self.dataset_id)
            errors = self.client.insert_rows(table, batch)
            logging.info("Errors %s", errors)
            if len(errors) > 0:
                LOGGER.error("Exception while committing files to bigquery. First item in batch: %s",
                             items_in_batch[0])
                raise Exception(errors)
            logging.info("Batch chunk committed.")
        log_execution_time(insert_start_time, "Insert rows to BigQuery took: %s")


if __name__ == '__main__':
    etl = CeidgEtl("1980-01-01", "simplify-docs", "")
    etl.run()
    # 3835 - 15 days
    # 161s      1 days - 4923 1 t
    # 44s       1 days - 8758 10t
    # 51s       1 days - 16740 10t
    # 183s      3 mth  - 33163 25t
    # 161s      3 mth  - 49586 20t 7days batch
