# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import Optional


@dataclass
class Address:
    city: Optional[str]
    code: Optional[str]
    country: Optional[str]
    house_no: Optional[str]
    post_office: Optional[str]
    street: Optional[str]


@dataclass
class CEO:
    first_name: Optional[str]
    krs_person_id: Optional[int]
    last_name: Optional[str]
    name: Optional[str]


@dataclass
class RejestrIoRecord:
    address: Optional[Address]
    ceo: Optional[CEO]
    business_insert_date: Optional[str]
    current_relations_count: Optional[int]
    data_fetched_at: Optional[str]
    duns: Optional[str]
    first_entry_date: Optional[str]
    historical_relations_count: Optional[int]
    id: Optional[int]
    is_opp: Optional[bool]
    is_removed: Optional[bool]
    krs: Optional[str]
    last_entry_date: Optional[str]
    last_entry_no: Optional[int]
    last_state_entry_date: Optional[str]
    last_state_entry_no: Optional[int]
    legal_form: Optional[str]
    name: Optional[str]
    name_short: Optional[str]
    ngo_insert_date: Optional[str]
    nip: Optional[str]
    regon: Optional[str]
    remove_validation_date: Optional[str]
    type: Optional[str]
    w_likwidacji: Optional[bool]
    w_upadlosci: Optional[bool]
    w_zawieszeniu: Optional[bool]
