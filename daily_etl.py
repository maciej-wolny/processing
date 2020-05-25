import datetime
from zeep import Client
import xmltodict
import time
import pandas as pd

API_REGON_BASE_URL = 'https://wyszukiwarkaregon.stat.gov.pl/wsBIR/wsdl/UslugaBIRzewnPubl-ver11-prod.wsdl'
API_REGON_TOKEN = 'bec4a0d036174eb488fb'

REPORT_TYPES = ['BIR11NowePodmiotyPrawneOrazDzialalnosciOsFizycznych',
                'BIR11AktualizowanePodmiotyPrawneOrazDzialalnosciOsFizycznych',
                'BIR11SkreslonePodmiotyPrawneOrazDzialalnosciOsFizycznych'
                ]
DAYS = 3


class ApiRegon:
    def __init__(self):
        self.session_key = None
        self.url = API_REGON_BASE_URL
        self._connect_to_api_regon()
        self.dates_list = []
        self.final_list = []
        self.regons_list = None

    def _connect_to_api_regon(self):
        """Creates ApiRegon session and stores session_key"""
        self.client = Client(self.url)
        login_data = {'pKluczUzytkownika': API_REGON_TOKEN}
        self.session_key = self.client.service.Zaloguj(**login_data)
        self.client.transport.session.headers.update({'sid': self.session_key})

    def search_by_regon(self, regon_number):
        """Searches ApiRegon by REGON number."""
        request_data = {'pParametryWyszukiwania': {'Regony9zn': regon_number}}
        result = self.client.service.DaneSzukajPodmioty(**request_data)
        return xmltodict.parse(result)

    def get_full_report(self, regon_number, report_type):
        """Gets full company report for specified REGON."""
        request_data = {'pRegon': regon_number,
                        'pNazwaRaportu': report_type}
        result = self.client.service.DanePobierzPelnyRaport(**request_data)
        return xmltodict.parse(result)

    def get_changes(self, date, report_type):
        """Gets REGON numbers for which data has changes in specified date for
        specified report-type"""
        request_data = {'pDataRaportu': date,
                        'pNazwaRaportu': report_type}
        result = self.client.service.DanePobierzRaportZbiorczy(**request_data)
        return xmltodict.parse(result)

    def generate_dates(self):
        first_date = datetime.datetime.now() - datetime.timedelta(days=DAYS)
        last_date = first_date + datetime.timedelta(days=DAYS)
        step = datetime.timedelta(days=1)
        while first_date < last_date:
            self.dates_list.append(first_date.strftime('%Y-%m-%d'))
            first_date += step

    def get_regons(self):
        for report_type in REPORT_TYPES:
            print(report_type)
            for date in self.dates_list:
                print(date)
                response = self.get_changes(date, report_type)['root']['dane']
                if not isinstance(response, list):
                    if 'ErrorCode' in response.keys():
                        print(response)
                    else:
                        self.normalize(response,
                                       date,
                                       report_type)
                else:
                    self.normalize(response,
                                   date,
                                   report_type)
                time.sleep(0.3)

    def normalize(self, response, date, report_type):
        if isinstance(response, dict):
            self.final_list.append({'regon': response['regon'],
                                    'date': date,
                                    'report_type': report_type})
        else:
            for item in response:
                self.final_list.append({'regon': item['regon'],
                                        'date': date,
                                        'report_type': report_type})

    def get_regons_list(self):
        df = self.final_df[(self.final_df['report_type'] ==
                            'BIR11NowePodmiotyPrawneOrazDzialalnosciOsFizycznych') |
                           (self.final_df['report_type'] ==
                            'BIR11AktualizowanePodmiotyPrawneOrazDzialalnosciOsFizycznych')]
        self.regons_list = df['regon'].to_list()

    def runner(self):
        self.generate_dates()
        self.get_regons()
        self.final_df = pd.DataFrame(self.final_list)
        self.get_regons_list()


API_CEIDG_URL = 'https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/' \
                'DataStoreProvider201901.svc?wsdl'
API_CEIDG_TOKEN = '9MEGSBAE0dU5U6PlS9uJnQmEPJabgsbzn9Hkj+4UWKlb88InD1OYWE' \
                  'C74ZW3TDFR'


class ApiCEIDG:
    # pylint: disable=too-few-public-methods
    def __init__(self):
        self.url = API_CEIDG_URL
        self.client = Client(self.url)

    def search_by_regon(self, regon):
        """Required date format: YYYY-MM-DD"""
        request_data = {'AuthToken': API_CEIDG_TOKEN,
                        'REGON': regon
                        }

        with self.client.settings(xml_huge_tree=True):
            try:
                response = xmltodict.parse(self.client.service.GetMigrationData201901(**request_data))
            except Fault as error:
                response = error.detail
        return response


class DailyETL:

    def __init__(self):
        self.ceidg_records = []
        self.krs_records = []
        self.ceidg_client = ApiCEIDG()
        self.regon_client = ApiRegon()

    def get_data(self, regons_list):
        counter = 1
        for regon in regons_list:
            ceidg_response = self.ceidg_client.search_by_regon(regon)
            if ceidg_response['WynikWyszukiwania'] is None:
                regon_response = self.regon_client.get_full_report(regon, 'BIR11OsPrawna')
                self.krs_records.append(regon_response)
                time.sleep(0.3)
            else:
                self.ceidg_records.append(ceidg_response)

            print('Processed {}/{}'.format(counter, len(regons_list)))
            counter += 1

    def run(self, regons_list):
        self.get_data(regons_list)
        #load to BQ


if __name__ == '__main__':
    etl = DailyETL()
    etl.regon_client.runner()
    regons = etl.regon_client.regons_list
    start = time.time()
    etl.run(regons)
    end = time.time()
    print(end - start)
