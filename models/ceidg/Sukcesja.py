# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import Optional


@dataclass
class Zarzadca:
    DataUstanowieniaZarzadcy: Optional[str]
    ImieZarzadcy: Optional[str]
    NazwiskoZarzadcy: Optional[str]
    NIP: Optional[str]
    ObywatelstwaZarzadcy: Optional[str]

@dataclass
class Sukcesja:
    DataUstanowieniaZarzadu: Optional[str]
    DataWygasnieciaZarzadu: Optional[str]
    Zarzadca: Optional[Zarzadca]
