# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass


@dataclass
class KRSRecord:
    address_city: str
    address_code: str
    address_house_no: str
    address_street: str
    district_court: str
    krs: str
    name: str
    nip: str
    regon: str
    shared_capital: str
