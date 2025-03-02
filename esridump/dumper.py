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
        """Iterates through all features in the ESRI layer efficiently."""
        query_fields = self._fields
        metadata = self.get_metadata()
        page_size = min(self._max_page_size, metadata.get("maxRecordCount", 500))
        row_count = None
    
        try:
            row_count = self.get_feature_count()
        except EsriDownloadError:
            self._logger.info("Source does not support feature count.")
    
        # If there are no records matching the query, return an empty generator
        if row_count == 0:
            return
            yield
    
        page_args = []
    
        if not self._paginate_oid and row_count is not None and (
            metadata.get("supportsPagination") or 
            metadata.get("advancedQueryCapabilities", {}).get("supportsPagination")
        ):
            # If the layer supports pagination, use resultOffset/resultRecordCount
            if query_fields and not self.can_handle_pagination(query_fields):
                self._logger.info(
                    "Source does not support pagination with fields specified, using all fields."
                )
                query_fields = None
    
            for offset in range(self._startWith, row_count, page_size):
                query_args = self._build_query_args({
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                    "where": "1=1",
                    "geometryPrecision": self._precision,
                    "returnGeometry": self._request_geometry,
                    "outSR": self._outSR,
                    "outFields": ",".join(query_fields or ["*"]),
                    "f": "json",
                })
                page_args.append(query_args)
    
            self._logger.info("Built %s requests using pagination", len(page_args))
        
        else:
            # If pagination isn't available, use WHERE clause with OIDs
            use_oids = True
            oid_field_name = self._find_oid_field_name(metadata)
    
            if not oid_field_name:
                raise EsriDownloadError("Could not find Object ID field.")
    
            if metadata.get("supportsStatistics"):
                try:
                    (oid_min, oid_max) = self._get_layer_min_max(oid_field_name)
    
                    for page_min in range(oid_min - 1, oid_max, page_size):
                        page_max = min(page_min + page_size, oid_max)
                        query_args = self._build_query_args({
                            "where": f"{oid_field_name} > {page_min} AND {oid_field_name} <= {page_max}",
                            "geometryPrecision": self._precision,
                            "returnGeometry": self._request_geometry,
                            "outSR": self._outSR,
                            "outFields": ",".join(query_fields or ["*"]),
                            "f": "json",
                        })
                        page_args.append(query_args)
    
                    self._logger.info("Built %s requests using OID WHERE clause", len(page_args))
                    use_oids = False
    
                except EsriDownloadError:
                    self._logger.warning("Failed to retrieve min/max OID. Trying OID enumeration.")
    
            if use_oids:
                try:
                    oids = sorted(map(int, self._get_layer_oids()))
    
                    for i in range(0, len(oids), page_size):
                        page_min = oids[i]
                        page_max = oids[min(i + page_size, len(oids) - 1)]
                        query_args = self._build_query_args({
                            "where": f"{oid_field_name} >= {page_min} AND {oid_field_name} <= {page_max}",
                            "geometryPrecision": self._precision,
                            "returnGeometry": self._request_geometry,
                            "outSR": self._outSR,
                            "outFields": ",".join(query_fields or ["*"]),
                            "f": "json",
                        })
                        page_args.append(query_args)
    
                    self._logger.info("Built %s requests using OID enumeration", len(page_args))
    
                except EsriDownloadError:
                    self._logger.warning("Falling back to spatial queries.")
                    bounds = metadata["extent"]
                    saved = set()
    
                    for feature in self._scrape_an_envelope(bounds, self._outSR, page_size):
                        attrs = feature["attributes"]
                        oid = attrs.get(oid_field_name)
                        if oid in saved:
                            continue
                        yield esri2geojson(feature)
                        saved.add(oid)
    
                    return
    
        # Process each query batch
        query_url = self._build_url("/query")
        headers = self._build_headers()
    
        for query_index, query_args in enumerate(page_args, start=1):
            download_exception = None
            data = None
    
            for retry in range(self._num_of_retry):
                try:
                    if query_index % self._requests_to_pause == 0:
                        time.sleep(self._pause_seconds)
                        self._logger.info("Pausing for %s seconds...", self._pause_seconds)
    
                    response = self._request("POST", query_url, headers=headers, data=query_args)
                    data = self._handle_esri_errors(response, "Error retrieving features")
                    download_exception = None
                    break  # Exit retry loop if successful
    
                except (socket.timeout, ValueError, requests.exceptions.RequestException) as e:
                    download_exception = EsriDownloadError(f"Error retrieving features: {e}")
                    time.sleep(self._pause_seconds * (retry + 1))
                    self._logger.warning("Retrying request... attempt %s", retry + 1)
    
            if download_exception:
                raise download_exception
    
            error = data.get("error")
            if error:
                raise EsriDownloadError(f"ESRI API error: {error['message']}")
    
            features = data.get("features", [])
    
            for feature in features:
                if self._output_format == "geojson":
                    yield esri2geojson(feature)
                else:
                    yield feature

