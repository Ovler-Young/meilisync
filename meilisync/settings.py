from typing import Dict, List

from pydantic import BaseModel, Extra
from pydantic_settings import BaseSettings

from meilisync.enums import IndexType, ProgressType, SourceType
from meilisync.plugin import load_plugin


class Source(BaseModel):
    type: SourceType
    database: str

    class Config:
        extra = Extra.allow


class MeiliSearch(BaseModel):
    api_url: str
    api_key: str | None = None
    insert_size: int | None = None
    insert_interval: int | None = None
    wait_for_task_timeout: int | None = None
    wait_for_task_interval: int = 500


class BasePlugin(BaseModel):
    plugins: List[str] = []

    def plugins_cls(self):
        plugins = []
        for plugin in self.plugins or []:
            p = load_plugin(plugin)
            if p.is_global:
                plugins.append(p())
            else:
                plugins.append(p)
        return plugins


class Sync(BasePlugin):
    table: str
    pk: str = "id"
    full: bool = False
    index: str | None = None
    fields: dict | None = None
    attributes: Dict[str, List[IndexType]] | None = None

    @property
    def index_name(self):
        return self.index or self.table

    @property
    def index_attributes(self) -> Dict[str, List[str]]:
        """Get a dictionary of index attribute types and their corresponding fields.

        Returns:
            Dict[str, List[str]]: Dictionary with keys 'searchable', 'sortable', 'filterable'
                                  and values as lists of field names with those attributes.
        """
        result: Dict[str, List[str]] = {"searchable": [], "sortable": [], "filterable": []}

        if not self.attributes:
            return result

        for field, types in self.attributes.items():
            if IndexType.searchable in types:
                result["searchable"].append(field)
            if IndexType.sortable in types:
                result["sortable"].append(field)
            if IndexType.filterable in types:
                result["filterable"].append(field)

        return result

    def __hash__(self):
        return hash(self.table)


class Progress(BaseModel):
    type: ProgressType

    class Config:
        extra = Extra.allow


class Sentry(BaseModel):
    dsn: str
    environment: str = "production"


class Settings(BaseSettings, BasePlugin):
    progress: Progress
    debug: bool = False
    source: Source
    meilisearch: MeiliSearch
    sync: List[Sync]
    sentry: Sentry | None = None

    @property
    def tables(self):
        return [sync.table for sync in self.sync]

    def get_sync(self, table: str):
        for sync in self.sync:
            if sync.table == table:
                return sync
