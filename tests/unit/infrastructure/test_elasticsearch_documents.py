from event_platform.infrastructure.elasticsearch.documents import (
    from_source,
    metadata_text,
    to_document,
)
from tests.factories import make_event


def test_metadata_text_flattens_nested_leaf_values() -> None:
    metadata = {
        "browser": "Firefox",
        "device": {"type": "mobile", "os": {"name": "iOS", "version": 17}},
        "tags": ["spring-sale", None, True],
        "empty": None,
    }

    assert metadata_text(metadata) == "Firefox mobile iOS 17 spring-sale True"


def test_document_round_trips_to_the_same_event() -> None:
    event = make_event(metadata={"campaign": {"name": "launch"}})

    document = to_document(event)

    assert document["metadata_text"] == "launch"
    assert from_source(document) == event
