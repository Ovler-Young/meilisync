from meilisync.schemas import Event
from meilisync.settings import Sync


class EventCollection:
    def __init__(self):
        self._events = {}
        self._ordered_events = []

    def add_event(self, sync: Sync, event: Event):
        pk = event.data[sync.pk]
        self._events.setdefault(sync, {})
        self._events[sync].setdefault(pk, [])
        self._events[sync][pk].append(event)
        self._ordered_events.append((sync, event))

    @property
    def size(self):
        return sum([len(events) for events in self._events.values()])

    @property
    def pop_ordered_events(self):
        events = self._ordered_events
        self._events = {}
        self._ordered_events = []
        return events
