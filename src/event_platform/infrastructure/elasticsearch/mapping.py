"""Elasticsearch index definition for events.

Field choices (see ARCHITECTURE.md for the full rationale):

* ``dynamic: strict`` - free-form metadata must never grow the mapping; unknown
  top-level fields are rejected instead of silently creating new mappings.
* ``event_type``, ``user_id``, ``event_id`` - ``keyword``: exact-match filters and
  aggregations, never full-text.
* ``source_url`` - ``keyword`` for exact filtering plus a ``text`` sub-field that
  splits the URL on punctuation, so "pricing" matches ``https://x.io/pricing``.
* ``metadata`` - ``flattened``: one field mapping for arbitrary JSON, which avoids
  mapping explosion while still allowing exact ``metadata.<key>`` lookups.
* ``metadata_text`` - every metadata leaf value concatenated at index time and
  analysed for full-text search. Lowercase + ASCII folding but *no stemming*:
  metadata values are mostly identifiers (browsers, devices, campaign names) where
  stemming would conflate distinct values.
"""

from typing import Any

METADATA_TEXT_ANALYZER = "metadata_text"
URL_TEXT_ANALYZER = "url_text"

ANALYSIS: dict[str, Any] = {
    "analyzer": {
        METADATA_TEXT_ANALYZER: {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["lowercase", "asciifolding"],
        },
        URL_TEXT_ANALYZER: {
            "type": "custom",
            "tokenizer": "url_parts",
            "filter": ["lowercase"],
        },
    },
    "tokenizer": {"url_parts": {"type": "pattern", "pattern": "[^\\p{L}\\p{N}]+"}},
}

MAPPINGS: dict[str, Any] = {
    "dynamic": "strict",
    "properties": {
        "event_id": {"type": "keyword"},
        "event_type": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "user_id": {"type": "keyword"},
        "source_url": {
            "type": "keyword",
            "ignore_above": 2048,
            "fields": {"text": {"type": "text", "analyzer": URL_TEXT_ANALYZER}},
        },
        "metadata": {"type": "flattened", "ignore_above": 1024},
        "metadata_text": {"type": "text", "analyzer": METADATA_TEXT_ANALYZER},
    },
}


def index_settings(number_of_shards: int, number_of_replicas: int) -> dict[str, Any]:
    return {
        "number_of_shards": number_of_shards,
        "number_of_replicas": number_of_replicas,
        "analysis": ANALYSIS,
    }
