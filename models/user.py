# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from enum import Enum
from dataclasses import dataclass


class Provider(Enum):
    GOOGLE = 0
    MICROSOFT = 1
    FACEBOOK = 2


class SubscriptionStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


@dataclass
class User:
    provider: int
    subscription: str
    mail: str
    name: str
    surname: str
    user_id: str
    picture: str
    token: str
