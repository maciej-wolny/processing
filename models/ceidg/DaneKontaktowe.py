# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import Optional


@dataclass
class DaneKontaktowe:
    AdresPocztyElektronicznej: Optional[str]
    AdresStronyInternetowej: Optional[str]
    Telefon: Optional[str]
    Faks: Optional[str]
