# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional
from src.models.ceidg.DaneAdresowe import DaneAdresowe
from src.models.ceidg.DaneDodatkowe import DaneDodatkowe
from src.models.ceidg.DaneKontaktowe import DaneKontaktowe
from src.models.ceidg.DanePodstawowe import DanePodstawowe
from src.models.ceidg.InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego \
    import ListaInformacji
from src.models.ceidg.SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca \
    import ListaInformacjiOSpolce
from src.models.ceidg.Sukcesja import Sukcesja
from src.models.ceidg.Uprawnienia import Uprawnienia
from src.models.ceidg.Zakazy import ListaInformacjiOZakazie


@dataclass
class InformacjaOWpisie:
    IdentyfikatorWpisu: str
    DanePodstawowe: DanePodstawowe
    DaneKontaktowe: DaneKontaktowe
    DaneAdresowe: DaneAdresowe
    DaneDodatkowe: DaneDodatkowe
    DataZgonu: Optional[str]
    Sukcesja: Optional[Sukcesja]
    Zakazy: Optional[ListaInformacjiOZakazie]
    SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca: \
        Optional[ListaInformacjiOSpolce]
    InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego: \
        Optional[ListaInformacji]
    Uprawnienia: Optional[Uprawnienia]


@dataclass
class WynikWyszukiwania:
    InformacjaOWpisie: List[InformacjaOWpisie]


@dataclass
class CEIDGResponse:
    WynikWyszukiwania: WynikWyszukiwania
