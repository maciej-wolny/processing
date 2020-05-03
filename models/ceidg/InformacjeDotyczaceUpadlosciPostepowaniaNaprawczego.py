# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Informacja:
    DataOrzeczeniaWszczeciaPostepowaniaNaprawczego: Optional[str]
    RodzajInformacji: Optional[str]
    SygnaturaSprawy: Optional[str]
    OrganWprowadzajacy: Optional[str]


@dataclass
class ListaInformacji:
    Informacja: List[Informacja]


@dataclass
class InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego:
    Informacja: Informacja
