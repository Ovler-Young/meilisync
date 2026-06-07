from meilisync.enums import EventType
from meilisync.source.mongo import build_change_stream_pipeline, change_to_event


def test_change_stream_pipeline_filters_configured_tables():
    assert build_change_stream_pipeline(["items", "users"]) == [
        {
            "$match": {
                "operationType": {"$in": ["insert", "update", "delete", "replace"]},
                "ns.coll": {"$in": ["items", "users"]},
            }
        }
    ]


def test_update_event_includes_primary_key_and_removed_fields():
    event = change_to_event(
        {
            "operationType": "update",
            "ns": {"coll": "items"},
            "documentKey": {"_id": "item-1"},
            "updateDescription": {
                "updatedFields": {"name": "new"},
                "removedFields": ["obsolete"],
            },
        },
        {"token": 1},
    )

    assert event.type == EventType.update
    assert event.table == "items"
    assert event.data == {"name": "new", "obsolete": None, "_id": "item-1"}
    assert event.progress == {"resume_token": {"token": 1}}


def test_replace_event_reindexes_full_document():
    event = change_to_event(
        {
            "operationType": "replace",
            "ns": {"coll": "items"},
            "fullDocument": {"_id": "item-1", "name": "new"},
        },
        {"token": 2},
    )

    assert event.type == EventType.create
    assert event.data == {"_id": "item-1", "name": "new"}
