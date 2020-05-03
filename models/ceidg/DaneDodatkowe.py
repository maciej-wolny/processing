# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import Optional

@dataclass
class DaneDodatkowe:
    DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej: Optional[str]
    DataZawieszeniaWykonywaniaDzialalnosciGospodarczej: Optional[str]
    DataWznowieniaWykonywaniaDzialalnosciGospodarczej: Optional[str]
    DataZaprzestaniaWykonywaniaDzialalnosciGospodarczej: Optional[str]
    DataWykresleniaWpisuZRejeOptionalu: Optional[str]
    MalzenskaWspolnoscMajatkowa: Optional[str]
    Status: Optional[str]
    KodyPKD: Optional[str]
