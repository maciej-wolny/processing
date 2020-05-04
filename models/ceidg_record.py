# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional
from models.ceidg.DaneAdresowe import DaneAdresowe
from models.ceidg.DaneDodatkowe import DaneDodatkowe
from models.ceidg.DaneKontaktowe import DaneKontaktowe
from models.ceidg.DanePodstawowe import DanePodstawowe
from models.ceidg.InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego \
    import ListaInformacji
from models.ceidg.SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca \
    import ListaInformacjiOSpolce
from models.ceidg.Sukcesja import Sukcesja
from models.ceidg.Uprawnienia import Uprawnienia
from models.ceidg.Zakazy import ListaInformacjiOZakazie


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
