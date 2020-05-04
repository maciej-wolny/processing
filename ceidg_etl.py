import dataclasses
import gc
import threading
from datetime import datetime
import time
import logging
import pandas as pd

from dacite import from_dict
from google.cloud import bigquery
from zeep import Client, Settings
import xmltodict
from enum import Enum

from models.ceidg_record import InformacjaOWpisie

OPTIMAL_BIGQUERY_BATCH_SIZE = 500

PROJECT_ID = "simplify-docs-devint"
DATASET_ID = 'entrepreneurship_data.ceidg'
API_CEIDG_URL = 'https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/' \
                    'DataStoreProvider201901.svc?wsdl'
API_CEIDG_TOKEN = '9MEGSBAE0dU5U6PlS9uJnQmEPJabgsbzn9Hkj+4UWKlb88InD1OYWE' \
                      'C74ZW3TDFR'
MAXIMUM_FIRESTORE_BATCH_SIZE = 500

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


class CEIDG:
    instance = None

    def __init__(self, project_id, dataset_id):
        self._instance = bigquery.Client(project_id)
        self.table_id = "{}.{}".format(project_id,
                                       dataset_id)
        self.existing_dates = self.get_all_dates_from_bq()

    def get_all_dates_from_bq(self):
        """Queries BQ to check for last first_entry_date within already inserted data."""
        query_content = ('SELECT DISTINCT(DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej)'
                         ' FROM `{}`'.format(self.table_id))
        query_job = self._instance.query(query_content)
        rows = query_job.result()
        return rows.to_dataframe()

    def find_missing_dates(self):
        """Finds missing dates in already inserted data between 1990-01-08 and current date."""
        self.existing_dates['DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej'] = pd.to_datetime(
            self.existing_dates['DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej'])
        self.existing_dates.index = self.existing_dates[
            'DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej']
        self.existing_dates.sort_index(inplace=True)
        self.existing_dates.drop(columns=['DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej'])
        now = datetime.now()
        now.strftime("%Y-%m-%d")
        return pd.date_range(start='1990-01-08', end=now).difference(self.existing_dates.index)


class CeidgEtl:
    def __init__(self, missing_dates, project_id, dataset_id):
        self.semaphore = threading.Semaphore(3)
        self.lock = threading.Lock()
        self.client = bigquery.Client(project_id)
        self.dataset_id = dataset_id
        self.current_date_index = 0
        self.missing_dates = missing_dates
        self.current_page = 1
        self.processed = 0
        self.current_throttle = 0
        self.url = API_CEIDG_URL
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
        start_date = datetime.strftime(self.missing_dates[self.current_date_index], '%Y-%m-%d')
        # if start_date.strftime('%Y-%m-%d') == "1990-04-02":
        #     self.semaphore.release()
        #     raise Exception("End, processed %s", self.processed)
        end_date = start_date
        self.current_date_index += 1
        self.lock.release()
        self.semaphore.acquire()
        logging.info("Starting to process batch [{} - {}]".format(start_date,
                                                                  end_date))
        download_time = time.time()
        request_data = {'AuthToken': API_CEIDG_TOKEN,
                        'DateFrom': start_date,
                        'DateTo': end_date}
        try:
            ceidg_results = self.soap_client.service.GetMigrationData201901(**request_data)
        except ConnectionError:
            try:
                ceidg_results = self.soap_client.service.GetMigrationData201901(**request_data)
            except ConnectionError:
                self.semaphore.release()
                raise ConnectionError(
                    "batch [{} - {}]".format(start_date, end_date))
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
    missing_dates = CEIDG(PROJECT_ID,
                          DATASET_ID).find_missing_dates()
    etl = CeidgEtl(missing_dates,
                   PROJECT_ID,
                   DATASET_ID)
    etl.run()
    # 3835 - 15 days
    # 161s      1 days - 4923 1 t
    # 44s       1 days - 8758 10t
    # 51s       1 days - 16740 10t
    # 183s      3 mth  - 33163 25t
    # 161s      3 mth  - 49586 20t 7days batch
