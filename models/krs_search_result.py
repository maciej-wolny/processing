# pylint: disable=C0103
# pylint: disable=R0902
# rules irrelevant for models
from dataclasses import dataclass, field
from typing import Optional, List

from dacite import from_dict
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class SearchEntity:
    name: str
    krs: Optional[str]
    nip: Optional[str]


@dataclass_json
@dataclass
class SearchResult:
    results: List[SearchEntity] = field(default_factory=list)

    @staticmethod
    def from_stream(stream):
        """Converts firestore stream to SearchResult model."""
        results = []
        for doc in stream:
            results.append(from_dict(data_class=SearchEntity, data=doc.to_dict()))
        return SearchResult(results)
