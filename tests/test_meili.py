from types import SimpleNamespace

import pytest

from meilisync.enums import EventType
from meilisync.meili import Meili
from meilisync.plugin import Plugin
from meilisync.schemas import Event
from meilisync.settings import Sync


class ReplaceNamePlugin(Plugin):
    async def pre_event(self, event: Event):
        return Event(type=event.type, table=event.table, data={**event.data, "name": "changed"})


class FakeIndex:
    def __init__(self):
        self.documents = None
        self.primary_key = None

    async def add_documents(self, documents, primary_key):
        self.documents = documents
        self.primary_key = primary_key
        return SimpleNamespace(task_uid=1)


class FakeClient:
    def __init__(self, index):
        self.index_name = None
        self._index = index

    def index(self, index_name):
        self.index_name = index_name
        return self._index


@pytest.mark.asyncio
async def test_handle_events_by_type_uses_pre_event_return_value():
    index = FakeIndex()
    meili = Meili("http://localhost:7700", "", plugins=[ReplaceNamePlugin()])
    meili.client = FakeClient(index)
    sync = Sync(table="items", pk="id", fields={"id": None, "name": None})

    await meili.handle_events_by_type(
        sync,
        [Event(type=EventType.create, data={"id": 1, "name": "original"})],
        EventType.create,
    )

    assert index.documents == [{"id": 1, "name": "changed"}]


@pytest.mark.asyncio
async def test_add_data_to_index_uses_plugins_for_temp_index():
    index = FakeIndex()
    meili = Meili("http://localhost:7700", "", plugins=[ReplaceNamePlugin()])
    meili.client = FakeClient(index)
    sync = Sync(table="items", pk="id", fields={"id": None, "name": None})

    await meili.add_data_to_index("items_tmp", sync, [{"id": 1, "name": "original"}])

    assert meili.client.index_name == "items_tmp"
    assert index.documents == [{"id": 1, "name": "changed"}]
