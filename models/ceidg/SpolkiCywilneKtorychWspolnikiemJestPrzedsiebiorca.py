# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class InformacjeOSpolce:
    NIP: Optional[str]
    REGON: Optional[str]


@dataclass
class ListaInformacjiOSpolce:
    InformacjeOSpolce: List[InformacjeOSpolce]


@dataclass
class SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca:
    InformacjeOSpolce: List[InformacjeOSpolce]
