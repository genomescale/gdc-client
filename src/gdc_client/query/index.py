import logging
from collections.abc import Mapping
from json import dumps
from urllib import parse as urlparse

import requests
import tenacity

from gdc_client import exceptions
from gdc_client.common import config

log = logging.getLogger("query")


class GDCIndexClient:
    def __init__(self, uri, verify=True):
        self.uri = uri
        self.active_meta_endpoint = "/v0/files"
        self.metadata = dict()
        self.verify = verify

    def get_related_files(self, uuid):
        # type: (str) -> list[str]
        if uuid in self.metadata.keys():
            return self.metadata[uuid]["related_files"]
        return []

    def get_annotations(self, uuid):
        # type: (str) -> list[str]
        if uuid in self.metadata.keys():
            return self.metadata[uuid]["annotations"]
        return []

    def get_md5sum(self, uuid):
        # type: (str) -> str
        if uuid in self.metadata.keys():
            return self.metadata[uuid]["md5sum"]

    def get_filesize(self, uuid):
        # type: (str) -> int
        if uuid in self.metadata.keys():
            return int(self.metadata[uuid]["file_size"])

    def get_access(self, uuid):
        if uuid in self.metadata.keys():
            return self.metadata[uuid]["access"]

    @tenacity.retry(
        retry=tenacity.retry_if_not_exception_type(
            (exceptions.ClientError, requests.exceptions.HTTPError)
        ),
        wait=tenacity.wait_exponential(max=config.REQUEST_RETRY_MAX_WAIT),
        stop=tenacity.stop_after_attempt(config.REQUEST_RETRY_ATTEMPTS),
        reraise=True,
    )
    def _get_hits(self, url, metadata_query):
        """
        Get hits metadata from a given API endpoint

        Args:
            url (str): Endpoint URL
            metadata_query (dict): Metadata query dictionary

        Returns:
            list: hits from the response data
        """
        json_response = {}
        # using a POST request lets us avoid the MAX URL character length limit
        with requests.post(
            url,
            json=metadata_query,
            verify=self.verify,
            timeout=(config.REQUEST_CONNECT_TIMEOUT, config.REQUEST_READ_TIMEOUT),
        ) as response:
            if response is None:
                return []

            if response.status_code == requests.codes.ok:
                json_response = response.json()

            return json_response.get("data", {}).get("hits", [])

    def _get_metadata(self, uuids):
        """
        Capture the metadata of all the UUIDs while making as little open
        connections as possible.

        Args:
            uuids (list): A list of UUIDs of the files

        Return:
            dict: metadata information

            self.metadata = {
                str file_id: {
                    str       access
                    str       file_size
                    str       md5sum
                    List[str] annotations
                    List[str] related files
                }
            }
        """

        filters = {
            "op": "and",
            "content": [
                {
                    "op": "in",
                    "content": {
                        "field": "files.file_id",
                        # Sometimes a <type 'set'> is passed
                        "value": list(uuids),
                    },
                }
            ],
        }

        metadata_query = {
            "fields": "file_id,file_size,md5sum,annotations.annotation_id,"
            "metadata_files.file_id,index_files.file_id,access",
            "filters": dumps(filters),
            "from": "0",
            "size": str(len(uuids)),  # one big request
        }

        active_meta_url = urlparse.urljoin(self.uri, self.active_meta_endpoint)

        active_hits = self._get_hits(active_meta_url, metadata_query)

        if not active_hits:
            log.debug(
                "Unable to retrieve file metadata information. "
                "continuing downloading as if they were large files"
            )
            return self.metadata

        for h in active_hits:
            related_returns = h.get("index_files", []) + h.get("metadata_files", [])
            related_files = [r["file_id"] for r in related_returns]

            annotations = [a["annotation_id"] for a in h.get("annotations", [])]

            # set the metadata as a class data member so that it can be
            # references as much as needed without needing to calculate
            # everything over again
            if h["id"] not in self.metadata.keys():
                # don't want to overwrite
                self.metadata[h["id"]] = {
                    "access": h["access"],
                    "file_size": h["file_size"],
                    "md5sum": h["md5sum"],
                    "annotations": annotations,
                    "related_files": related_files,
                }

        return self.metadata

    def separate_small_files(
        self, file_positions: Mapping[str, int], chunk_size: int
    ) -> tuple[list[str], list[list[str]]]:
        """Separate big and small files

        Separate the small files from the larger files in
        order to combine them into single grouped downloads. This will reduce
        the number of open connections needed to be made for many small files.

        On top of that, separate the small files by open and controlled access
        so that if a controlled grouping failed, you can handle it as the same
        edge case.

        Args:
            file_positions: file UUIDs mapped to their first requested position
            chunk_size (int): the maximum allowed combined size of small files

        Return:
            list: big file UUIDs in their original input order
            list: a list of lists of UUIDs. Each inner list representing a
                group of small files
        """

        bigs = []
        smalls_by_access = {"open": [], "controlled": []}
        bundle_sizes = {"open": 0, "controlled": 0}

        log.debug("Grouping ids by size")

        self._get_metadata(file_positions)
        for uuid in sorted(file_positions, key=file_positions.__getitem__):
            if uuid not in self.metadata:
                bigs.append(uuid)
                continue

            rf = self.get_related_files(uuid)
            af = self.get_annotations(uuid)

            # Related files and annotations require an individual download.
            if rf or af:
                bigs.append(uuid)
                continue

            file_size = self.get_filesize(uuid)
            if file_size > chunk_size:
                bigs.append(uuid)
                continue

            access = self.get_access(uuid)
            if access not in smalls_by_access:
                continue
            groups = smalls_by_access[access]
            if not groups or bundle_sizes[access] > chunk_size:
                groups.append([])
                bundle_sizes[access] = 0
            groups[-1].append(uuid)
            bundle_sizes[access] += file_size

        smalls = smalls_by_access["open"] + smalls_by_access["controlled"]

        # for logging/reporting purposes
        total_count = len(bigs) + sum([len(s) for s in smalls])
        if len(file_positions) > total_count:
            log.warning("There are less files to download than originally given")
            log.warning(f"Number of files originally given: {len(file_positions)}")

        log.debug(f"{total_count} total number of files to download")
        log.debug(f"{len(smalls)} groupings of files")

        return bigs, smalls
