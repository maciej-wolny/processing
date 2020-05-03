# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import Optional


@dataclass
class DanePodstawowe:
    Imie: Optional[str]
    Nazwisko: Optional[str]
    NIP: Optional[str]
    REGON: Optional[str]
    Firma: Optional[str]
