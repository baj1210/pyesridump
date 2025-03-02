import aiohttp
import asyncio
import json
import logging
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

    async def _request(self, session, method, url, **kwargs):
        """Handles HTTP requests asynchronously with retries."""
        attempt = 0
        while attempt < self._num_of_retry:
            try:
                async with session.request(method, url, timeout=self._http_timeout, **kwargs) as response:
                    if response.status != 200:
                        raise EsriDownloadError(f"HTTP {response.status}: {await response.text()}")
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                attempt += 1
                wait_time = self._pause_seconds * (2 ** attempt)
                self._logger.warning(f"Request failed ({e}), retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)

        raise EsriDownloadError(f"Max retries reached for {url}")

    async def async_get_layer_oids(self, session):
        """Retrieve all ObjectIDs asynchronously."""
        query_args = {
            "where": "1=1",
            "returnIdsOnly": "true",
            "f": "json",
        }
        url = self._layer_url + "/query"

        data = await self._request(session, "GET", url, params=query_args, headers=self._headers)
        return sorted(map(int, data.get("objectIds", [])))

    async def get_metadata(self, session):
        """Retrieve feature layer metadata asynchronously."""
        return await self._request(session, "GET", self._build_url(), params={"f": "json"}, headers=self._headers)

    async def get_feature_count(self, session):
        """Get total feature count from the ESRI feature layer asynchronously."""
        query_args = self._build_query_args({
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json"
        })
        count_json = await self._request(session, "GET", self._build_url("/query"), params=query_args, headers=self._headers)
        return count_json.get("count", 0)

    async def _get_layer_oids(self, session):
        """Retrieve all ObjectIDs for pagination asynchronously."""
        query_args = self._build_query_args({
            "where": "1=1",
            "returnIdsOnly": "true",
            "f": "json",
        })
        oid_data = await self._request(session, "GET", self._build_url("/query"), params=query_args, headers=self._headers)
        return sorted(map(int, oid_data.get("objectIds", [])))

    async def fetch_features(self, session, query_args, queue):
        """Fetch features asynchronously for a given query and put them in a queue."""
        data = await self._request(session, "POST", self._build_url("/query"), json=query_args, headers=self._headers)
        for feature in data.get("features", []):
            await queue.put(feature)  # Put feature into queue for async iteration

    async def async_iter(self):
        """Iterates through all features asynchronously in parallel."""
        async with aiohttp.ClientSession() as session:
            metadata = await self.get_metadata(session)
            page_size = min(self._max_page_size, metadata.get("maxRecordCount", 500))
            row_count = await self.get_feature_count(session)

            if row_count == 0:
                return

            oids = await self._get_layer_oids(session)

            # Break OIDs into chunks for parallel requests
            oid_chunks = [oids[i:i + page_size] for i in range(0, len(oids), page_size)]
            queue = asyncio.Queue()

            async def producer():
                """Produce tasks to fetch feature data asynchronously."""
                tasks = []
                for chunk in oid_chunks:
                    query = {"where": f"OBJECTID IN ({','.join(map(str, chunk))})", "f": "json"}
                    tasks.append(self.fetch_features(session, query, queue))

                await asyncio.gather(*tasks)
                await queue.put(None)  # Signal that all tasks are done

            asyncio.create_task(producer())

            while True:
                feature = await queue.get()
                if feature is None:
                    break  # Stop when producer signals completion
                if self._output_format == "geojson":
                    yield esri2geojson(feature)
                else:
                    yield feature

    def _build_url(self, url=None):
        return self._layer_url + (url if url else "")

    def _build_query_args(self, query_args=None):
        complete_args = query_args.copy() if query_args else {}
        complete_args.update(self._query_params)
        return complete_args



