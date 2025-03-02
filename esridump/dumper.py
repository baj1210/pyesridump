import logging
import requests
import json
import socket
import time
import random
from six.moves.urllib.parse import urlencode

from esridump import esri2geojson
from esridump.errors import EsriDownloadError

class EsriDumper:
    def __init__(self, url, parent_logger=None,
                 extra_query_args=None, extra_headers=None,
                 timeout=30, fields=None, request_geometry=True,
                 outSR="4326", proxy=None,
                 start_with=0, geometry_precision=7,
                 paginate_oid=False, max_page_size=1000,
                 pause_seconds=10, requests_to_pause=5,
                 num_of_retry=5, output_format="geojson"):
        
        self._layer_url = url
        self._query_params = extra_query_args or {}
        self._headers = extra_headers or {}
        self._http_timeout = timeout
        self._fields = fields or None
        self._outSR = outSR
        self._request_geometry = request_geometry
        self._proxy = proxy
        self._startWith = start_with
        self._precision = geometry_precision
        self._paginate_oid = paginate_oid
        self._max_page_size = max_page_size

        self._pause_seconds = pause_seconds
        self._requests_to_pause = requests_to_pause
        self._num_of_retry = num_of_retry

        if output_format not in ("geojson", "esrijson"):
            raise ValueError(f'Invalid output format. Expecting "geojson" or "esrijson", got {output_format}')

        self._output_format = output_format

        if parent_logger:
            self._logger = parent_logger.getChild("esridump")
        else:
            self._logger = logging.getLogger("esridump")

    def _request(self, method, url, **kwargs):
        """Handles HTTP requests with exponential backoff for reliability."""
        attempt = 0
        while attempt < self._num_of_retry:
            try:
                if self._proxy:
                    url = self._proxy + url

                    params = kwargs.pop("params", None)
                    if params:
                        url += "?" + urlencode(params)

                self._logger.debug("%s %s, args %s", method, url, kwargs.get("params") or kwargs.get("data"))
                return requests.request(method, url, timeout=self._http_timeout, **kwargs)

            except requests.exceptions.RequestException as e:
                attempt += 1
                wait_time = self._pause_seconds * (2 ** attempt) + random.uniform(0, 1)
                self._logger.warning(f"Request failed ({e}), retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)

        raise EsriDownloadError(f"Max retries reached for {url}")

    def _build_url(self, url=None):
        return self._layer_url + (url if url else "")

    def _build_query_args(self, query_args=None):
        complete_args = query_args.copy() if query_args else {}
        complete_args.update(self._query_params)
        return complete_args

    def _handle_esri_errors(self, response, error_message):
        if response.status_code != 200:
            raise EsriDownloadError(f'{response.request.url}: {error_message} HTTP {response.status_code} {response.text}')

        try:
            data = response.json()
        except json.JSONDecodeError:
            self._logger.error(f"Could not parse response from {response.request.url} as JSON:\n\n{response.text}")
            raise

        error = data.get("error")
        if error:
            raise EsriDownloadError(f"{error_message}: {error['message']} {', '.join(error.get('details', []))}")

        return data

    def get_metadata(self):
        """Retrieve feature layer metadata."""
        response = self._request("GET", self._build_url(), params={"f": "json"}, headers=self._headers)
        return self._handle_esri_errors(response, "Could not retrieve layer metadata")

    def get_feature_count(self):
        """Get total feature count from the ESRI feature layer."""
        query_args = self._build_query_args({
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json"
        })
        response = self._request("GET", self._build_url("/query"), params=query_args, headers=self._headers)
        count_json = self._handle_esri_errors(response, "Could not retrieve row count")
        return count_json.get("count", 0)

    def _get_layer_oids(self):
        """Retrieve all ObjectIDs for pagination."""
        query_args = self._build_query_args({
            "where": "1=1",
            "returnIdsOnly": "true",
            "f": "json",
        })
        response = self._request("GET", self._build_url("/query"), params=query_args, headers=self._headers)
        oid_data = self._handle_esri_errors(response, "Could not retrieve object IDs")
        return sorted(map(int, oid_data.get("objectIds", [])))

    def __iter__(self):
        metadata = self.get_metadata()
        page_size = min(self._max_page_size, metadata.get("maxRecordCount", 500))

        row_count = self.get_feature_count()
        if row_count == 0:
            return

        page_args = []

        if metadata.get("supportsPagination"):
            for offset in range(self._startWith, row_count, page_size):
                query_args = self._build_query_args({
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                    "where": "1=1",
                    "geometryPrecision": self._precision,
                    "returnGeometry": self._request_geometry,
                    "outSR": self._outSR,
                    "outFields": ",".join(self._fields or ["*"]),
                    "f": "json",
                })
                page_args.append(query_args)

        else:
            oid_field_name = metadata.get("objectIdField")
            if not oid_field_name:
                raise EsriDownloadError("Could not find ObjectID field")

            oids = self._get_layer_oids()
            for i in range(0, len(oids), page_size):
                query_args = self._build_query_args({
                    "where": f"{oid_field_name} >= {oids[i]} AND {oid_field_name} <= {oids[min(i+page_size, len(oids)-1)]}",
                    "geometryPrecision": self._precision,
                    "returnGeometry": self._request_geometry,
                    "outSR": self._outSR,
                    "outFields": ",".join(self._fields or ["*"]),
                    "f": "json",
                })
                page_args.append(query_args)

        query_url = self._build_url("/query")
        for query_args in page_args:
            response = self._request("POST", query_url, headers=self._headers, data=query_args)
            data = self._handle_esri_errors(response, "Could not retrieve features")

            for feature in data.get("features", []):
                yield esri2geojson(feature) if self._output_format == "geojson" else feature

