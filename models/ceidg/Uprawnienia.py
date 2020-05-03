# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Uprawnienia:
    Uprawnienie: Optional[List[str]]
