import logging
import json
import sys
from typing import Any, Dict, List
import requests as req
import singer

logger = singer.get_logger()

# Require only BASE_URL (Redash base), API_KEY, QUERY_ID
REQUIRED_CONFIG_KEYS = ['BASE_URL', 'API_KEY', 'QUERY_ID']
args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)


class Redash:
    """
    Minimal Singer tap for fetching a single Redash query result using API key auth.
    - Auth via API key only.
    - Requires only BASE_URL, API_KEY, QUERY_ID.
    - Generates a Singer schema by scanning sample rows.
    - --discover prints a simplified wrapper: { "stream": ..., "schema": ..., "key_properties": [...] }
    - No STATE/incremental support by design.
    """

    def __init__(self) -> None:
        try:
            self._config: Dict[str, Any] = args.config
        except Exception as e:
            raise IOError(e)

        self._session = req.Session()
        self._timeout = (10, 60)  # (connect, read) seconds

        self.query_id: str = str(self._config['QUERY_ID'])
        self._data = self._get_query_data(self.query_id)

    # -------- Fetch Query Data -------- #

    def _get_query_data(self, query_id: str) -> List[Dict[str, Any]]:
        """Fetch results for the given Redash query as a list of row dicts."""
        base = self._config['BASE_URL'].rstrip('/')
        url = f"{base}/api/queries/{query_id}/results.json"
        params = {'api_key': self._config['API_KEY']}
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            payload = resp.json()
        except req.RequestException as e:
            logger.critical("Error fetching Redash query results: %s", e)
            raise
        except ValueError as e:
            logger.critical("Invalid JSON from Redash results endpoint: %s", e)
            raise

        try:
            rows = payload['query_result']['data']['rows']
            if not isinstance(rows, list):
                raise TypeError("Redash rows payload is not a list.")
        except (KeyError, TypeError) as e:
            logger.critical("Unexpected Redash payload shape: %s", e)
            raise

        logger.info("Fetched %d rows from Redash query %s.", len(rows), query_id)
        return rows

    # -------- Schema Inference -------- #

    @staticmethod
    def _singer_type_for_value(value: Any) -> str:
        """Map a Python value to a Singer JSON Schema primitive type (excluding 'null')."""
        if value is None:
            return None
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int) or isinstance(value, float):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, list):
            return "array"
        return "string"

    def _infer_properties(self, sample_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build Singer 'properties' by scanning up to N rows and forming a union type for each field."""
        MAX_SCAN = min(100, len(sample_rows))
        union: Dict[str, set] = {}

        for i in range(MAX_SCAN):
            row = sample_rows[i]
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                t = self._singer_type_for_value(v)
                if k not in union:
                    union[k] = set()
                if t is not None:
                    union[k].add(t)

        properties: Dict[str, Any] = {}
        for field, type_set in union.items():
            field_types = ["null"] + sorted(list(type_set)) if type_set else ["null", "string"]
            properties[field] = {"type": field_types}

        return properties

    def generate_schema_wrapper(self) -> Dict[str, Any]:
        """Return a simplified Singer schema wrapper for this stream."""
        if not self._data:
            properties: Dict[str, Any] = {}
        else:
            properties = self._infer_properties(self._data)

        key_props = self._config.get("key_properties", [])
        if not isinstance(key_props, list):
            key_props = []

        wrapper: Dict[str, Any] = {
            "stream": self.query_id,
            "schema": {
                "type": "object",
                "properties": properties,
                "additionalProperties": False,
            },
            "key_properties": key_props,
        }
        return wrapper

    # -------- Singer IO -------- #

    def do_discover(self) -> Dict[str, Any]:
        """Discovery mode prints the simplified schema wrapper and returns it."""
        wrapper = self.generate_schema_wrapper()
        print(json.dumps(wrapper, indent=2))
        return wrapper

    def output_to_stream(self, stream_name: str, schema_wrapper: Dict[str, Any]) -> None:
        """Emit schema and records to stdout in Singer format."""
        schema = schema_wrapper["schema"]
        key_props = schema_wrapper.get("key_properties", [])
        singer.write_schema(stream_name, schema, key_props)

        # Emit all records in one call to ensure they are written to stdout
        if self._data:
            singer.write_records(stream_name, self._data)


def main() -> None:
    rdash = Redash()

    if args.discover:
        rdash.do_discover()
        return

    schema_wrapper = args.properties if args.properties else rdash.generate_schema_wrapper()

    if not isinstance(schema_wrapper, dict) or "stream" not in schema_wrapper or "schema" not in schema_wrapper:
        logger.warning("Invalid or missing properties provided; regenerating schema.")
        schema_wrapper = rdash.generate_schema_wrapper()

    rdash.output_to_stream(schema_wrapper["stream"], schema_wrapper)

    # Ensure everything is flushed to stdout for downstream targets
    sys.stdout.flush()


if __name__ == "__main__":
    main()
