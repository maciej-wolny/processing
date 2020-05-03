# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Adres:
    TERC: Optional[str]
    SIMC: Optional[str]
    ULIC: Optional[str]
    Miejscowosc: Optional[str]
    Ulica: Optional[str]
    Budynek: Optional[str]
    Lokal: Optional[str]
    KodPocztowy: Optional[str]
    Poczta: Optional[str]
    Gmina: Optional[str]
    Powiat: Optional[str]
    Wojewodztwo: Optional[str]
    OpisNietypowegoMiejscaLokalizacji: Optional[str]


@dataclass
class ListaAdresow:
    Adres: List[Adres]


@dataclass
class DaneAdresowe:
    AdresGlownegoMiejscaWykonywaniaDzialalnosci: Adres
    AdresDoDoreczen: Adres
    PrzedsiebiorcaPosiadaObywatelstwaPanstw: Optional[str]
    AdresyDodatkowychMiejscWykonywaniaDzialalnosci: Optional[ListaAdresow]
