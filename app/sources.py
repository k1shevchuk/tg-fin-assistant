from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Protocol, Sequence


@dataclass(slots=True)
class IdeaSource:
    url: str
    name: str
    date: datetime


class SourceProvider(Protocol):
    def get_sources(self, *args, **kwargs) -> Sequence[IdeaSource]:
        ...


def filter_fresh_sources(
    sources: Iterable[IdeaSource],
    max_age_days: int,
    as_of: datetime | None = None,
) -> list[IdeaSource]:
    pivot = _to_utc(as_of or datetime.utcnow())
    fresh: list[IdeaSource] = []
    for item in sources:
        item_date = _to_utc(item.date)
        if (pivot - item_date).days <= max_age_days:
            fresh.append(item)
    return fresh


def _to_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)
