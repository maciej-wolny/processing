# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional, Dict


@dataclass
class InformacjaOZakazie:
    Typ: Optional[str]
    Opis: Optional[str]
    OkresNaJakiZostalOrzeczonyZakaz: Optional[str]
    ZakazWydal: Optional[str]
    Nazwa: Optional[str]
    SygnaturaAktSprawy: Optional[str]
    DataWydaniaOrzeczenia: Optional[str]
    DataUprawomocnieniaOrzeczenia: Optional[str]


@dataclass
class ListaInformacjiOZakazie:
    InformacjaOZakazie: List[InformacjaOZakazie]


@dataclass
class Zakazy:
    InformacjaOZakazie: Dict
