import asyncio
from contextlib import suppress
from typing import AsyncGenerator, List, Optional, Type, Union

import httpx
from loguru import logger
from meilisearch_python_sdk import AsyncClient
from meilisearch_python_sdk.errors import MeilisearchApiError

from meilisync.enums import EventType
from meilisync.event import EventCollection
from meilisync.plugin import Plugin
from meilisync.schemas import Event
from meilisync.settings import Sync


class Meili:
    def __init__(
        self,
        api_url: str,
        api_key: str,
        plugins: Optional[List[Union[Type[Plugin], Plugin]]] = None,
        wait_for_task_timeout: Optional[int] = None,
        wait_for_task_interval: int = 500,
    ):
        self.client = AsyncClient(
            api_url,
            api_key,
        )
        self.plugins = plugins or []
        self.wait_for_task_timeout = wait_for_task_timeout
        self.wait_for_task_interval = wait_for_task_interval

    async def add_data(self, sync: Sync, data: list):
        events = [Event(type=EventType.create, data=item) for item in data]
        return await self.handle_events_by_type(sync, events, EventType.create)

    async def add_data_to_index(
        self, index_name: str, pk: str, data: list, fields: dict | None = None
    ):
        index = self.client.index(index_name)
        documents = [Event(type=EventType.create, data=item).mapping_data(fields) for item in data]
        task = await index.add_documents(documents, primary_key=pk)
        return task

    async def handle_events_by_type(
        self,
        sync: Sync,
        events: List[Event],
        event_type: EventType,
        index_name: str | None = None,
    ):
        if not events:
            return

        index_name = index_name or sync.index_name
        index = self.client.index(index_name)
        for event in events:
            await self.handle_plugins_pre(sync, event)

        task = None
        if event_type == EventType.create:
            task = await index.add_documents(
                [event.mapping_data(sync.fields) for event in events], primary_key=sync.pk
            )
        elif event_type == EventType.update:
            task = await index.update_documents(
                [event.mapping_data(sync.fields) for event in events], primary_key=sync.pk
            )
        elif event_type == EventType.delete:
            task = await index.delete_documents([str(event.data[sync.pk]) for event in events])

        for event in events:
            await self.handle_plugins_post(sync, event)
        return task

    async def handle_event(
        self,
        event: Event,
        sync: Sync,
        index_name: str | None = None,
    ):
        event = await self.handle_plugins_pre(sync, event)
        index = self.client.index(index_name or sync.index_name)
        if event.type == EventType.create:
            task = await index.add_documents([event.mapping_data(sync.fields)], primary_key=sync.pk)
        elif event.type == EventType.update:
            task = await index.update_documents(
                [event.mapping_data(sync.fields)], primary_key=sync.pk
            )
        elif event.type == EventType.delete:
            task = await index.delete_documents([str(event.data[sync.pk])])
        else:
            task = None
        await self.handle_plugins_post(sync, event)
        return task

    async def handle_events(self, collection: EventCollection, index_name: str | None = None):
        tasks = []
        for sync, event in collection.pop_ordered_events:
            task = await self.handle_event(event, sync, index_name=index_name)
            if task:
                tasks.append(task)
        return tasks

    async def wait_for_task_with_retry(self, task_uid):
        while True:
            try:
                await self.client.wait_for_task(
                    task_id=task_uid,
                    timeout_in_ms=self.wait_for_task_timeout,
                    interval_in_ms=self.wait_for_task_interval,
                )
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 408:
                    logger.debug(f"Task {task_uid} timeout (408), retrying wait...")
                    await asyncio.sleep(1)
                    continue
                raise

    async def wait_for_tasks(self, tasks: list, concurrency: int = 3):
        sem = asyncio.Semaphore(concurrency)

        async def wait_with_sem(task_uid):
            async with sem:
                await self.wait_for_task_with_retry(task_uid)

        await asyncio.gather(*(wait_with_sem(task.task_uid) for task in tasks if task))

    async def refresh_data(
        self,
        sync: Sync,
        data: AsyncGenerator,
        source=None,
        collection=None,
        meili_settings=None,
        progress=None,
        initial_progress=None,
    ):
        index = sync.index_name
        pk = sync.pk
        index_name_tmp = f"{index}_tmp"
        logger.info(f"Starting to delete index {index_name_tmp}...")
        try:
            task = await self.client.index(index_name_tmp).delete()
            await self.wait_for_task_with_retry(task.task_uid)
        except MeilisearchApiError as e:
            if e.code != "MeilisearchApiError.index_not_found":
                raise
        logger.info(f"Delete index {index_name_tmp} complete")
        logger.info(f"Starting to create index {index_name_tmp}...")
        settings = await self.client.index(index).get_settings()
        index_tmp = await self.client.create_index(index_name_tmp, primary_key=pk)
        task = await index_tmp.update_settings(settings)
        logger.info(f"Waiting for update tmp index {index_name_tmp} settings to complete...")
        await self.client.wait_for_task(
            task_id=task.task_uid,
            timeout_in_ms=self.wait_for_task_timeout,
            interval_in_ms=self.wait_for_task_interval,
        )
        logger.info(f"Create index {index_name_tmp} complete")
        logger.info(f"Starting to add data to index {index_name_tmp}...")
        tasks = [task]
        count = 0
        batch = 0
        current_progress = None
        async for items in data:
            batch += 1
            count += len(items)
            logger.debug(f"Batch {batch} sending...")
            task = await self.add_data_to_index(index_name_tmp, pk, items, sync.fields)
            tasks.append(task)

        sem = asyncio.Semaphore(3)

        async def wait_with_sem(task_uid):
            async with sem:
                await self.wait_for_task_with_retry(task_uid)

        wait_tasks = [wait_with_sem(item.task_uid) for item in tasks]
        logger.info(f"Waiting for insert tmp index {index_name_tmp} to complete...")

        # If source stream is provided, process events concurrently while waiting for tasks
        if source is not None and collection is not None and meili_settings is not None:
            current_progress = {}
            lock = asyncio.Lock()
            event_tasks = []
            all_tasks_done = False

            async def wait_for_all_tasks():
                nonlocal all_tasks_done
                await asyncio.gather(*wait_tasks)
                all_tasks_done = True
                logger.info(f"All insert tasks for tmp index {index_name_tmp} complete")

            async def process_refresh_event(event):
                nonlocal current_progress
                current_progress = event.progress
                if isinstance(event, Event):
                    event_sync = event.table == sync.table
                    if not event_sync:
                        return
                    if not meili_settings.insert_size and not meili_settings.insert_interval:
                        task = await self.handle_event(event, sync, index_name=index_name_tmp)
                        if task:
                            event_tasks.append(task)
                        await self.wait_for_tasks(event_tasks)
                        event_tasks.clear()
                    else:
                        collection.add_event(sync, event)
                        if (
                            meili_settings.insert_size
                            and collection.size >= meili_settings.insert_size
                        ):
                            async with lock:
                                event_tasks.extend(
                                    await self.handle_events(collection, index_name=index_name_tmp)
                                )
                                await self.wait_for_tasks(event_tasks)
                                event_tasks.clear()

            async def process_events():
                logger.info(f"Starting to process new events for tmp index {index_name_tmp}...")
                source_iter = source.__aiter__()
                next_event_task = asyncio.create_task(source_iter.__anext__())
                try:
                    while not all_tasks_done:
                        done, _ = await asyncio.wait({next_event_task}, timeout=1)
                        if not done:
                            continue
                        try:
                            event = next_event_task.result()
                        except StopAsyncIteration:
                            break
                        await process_refresh_event(event)
                        next_event_task = asyncio.create_task(source_iter.__anext__())
                finally:
                    if not next_event_task.done():
                        next_event_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await next_event_task
                    await source_iter.aclose()
                logger.info("All tasks done, stopping event processing for tmp index")

            # Run both tasks concurrently, but prioritize waiting for all tasks
            await asyncio.gather(wait_for_all_tasks(), process_events())

            # Process any remaining events in the collection
            if collection.size > 0:
                async with lock:
                    event_tasks.extend(
                        await self.handle_events(collection, index_name=index_name_tmp)
                    )
                    if current_progress:
                        await self.wait_for_tasks(event_tasks)
                        event_tasks.clear()

            if event_tasks:
                await self.wait_for_tasks(event_tasks)
        else:
            # Original behavior: just wait for all tasks
            await asyncio.gather(*wait_tasks)

        # Swap the temporary index with the original index
        task = await self.client.swap_indexes([(index, index_name_tmp)])
        logger.info(f"Waiting for swap index {index} to complete...")
        await self.client.wait_for_task(
            task_id=task.task_uid,
            timeout_in_ms=self.wait_for_task_timeout,
            interval_in_ms=self.wait_for_task_interval,
        )
        if progress:
            await progress.set(**current_progress or initial_progress)
        await self.client.index(index_name_tmp).delete()
        logger.success(f"Swap index {index} complete")
        return count

    async def get_count(self, index: str):
        stats = await self.client.index(index).get_stats()
        return stats.number_of_documents

    async def index_exists(self, index: str):
        try:
            await self.client.get_index(index)
            return True
        except MeilisearchApiError as e:
            if e.code == "index_not_found":
                return False
            raise e

    async def handle_plugins_pre(self, sync: Sync, event: Event):
        for plugin in self.plugins:
            if isinstance(plugin, Plugin):
                event = await plugin.pre_event(event)
            else:
                event = await plugin().pre_event(event)
        for plugin in sync.plugins_cls():
            if isinstance(plugin, Plugin):
                event = await plugin.pre_event(event)
            else:
                event = await plugin().pre_event(event)
        return event

    async def handle_plugins_post(self, sync: Sync, event: Event):
        for plugin in self.plugins:
            if isinstance(plugin, Plugin):
                event = await plugin.post_event(event)
            else:
                event = await plugin().post_event(event)
        for plugin in sync.plugins_cls():
            if isinstance(plugin, Plugin):
                event = await plugin.post_event(event)
            else:
                event = await plugin().post_event(event)
        return event
