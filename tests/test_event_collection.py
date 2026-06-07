from meilisync.enums import EventType
from meilisync.event import EventCollection
from meilisync.schemas import Event
from meilisync.settings import Sync


def test_size_counts_each_queued_event_for_same_primary_key():
    sync = Sync(table="items", pk="id", fields={"id": None})
    collection = EventCollection()

    collection.add_event(sync, Event(type=EventType.delete, data={"id": 1}))
    collection.add_event(sync, Event(type=EventType.create, data={"id": 1}))

    assert collection.size == 2
    assert [event.type for _, event in collection.pop_ordered_events] == [
        EventType.delete,
        EventType.create,
    ]
