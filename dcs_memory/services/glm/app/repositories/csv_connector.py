import asyncio
import csv
import os
import glob
import logging
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone

from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2
from dcs_memory.services.glm.app.config import ExternalDataSourceConfig, GLMConfig
from .external_base import BaseExternalRepository
from .base import StorageError, BackendUnavailableError, InvalidQueryError # Reusing exceptions

logger = logging.getLogger(__name__)

class CsvDirExternalRepository(BaseExternalRepository):
    """
    External repository implementation for reading KEM-like data from a directory of CSV files.
    """

    def __init__(self, config: ExternalDataSourceConfig, glm_config: GLMConfig):
        super().__init__(config, glm_config)

        # Validate and parse mapping_config for CSV directory
        mc = self.config.mapping_config
        self.directory_path: str = mc.get("directory_path")
        self.id_column_name: str = mc.get("id_column_name")
        self.content_column_names: List[str] = mc.get("content_column_names", [])
        self.timestamp_column_name: str = mc.get("timestamp_column_name") # Used for KEM updated_at and sorting

        self.metadata_column_names: List[str] = mc.get("metadata_column_names", [])
        self.csv_delimiter: str = mc.get("csv_delimiter", ",")
        self.encoding: str = mc.get("encoding", "utf-8")
        self.content_type_value: str = mc.get("content_type_value", "text/plain")
        # For KEM created_at, we can use the same as timestamp_column_name or make it configurable
        self.created_at_column_name: str = mc.get("created_at_column_name", self.timestamp_column_name)


        if not all([self.directory_path, self.id_column_name, self.content_column_names, self.timestamp_column_name]):
            raise ValueError(
                f"CSV connector '{self.config.name}': Missing required mapping_config fields: "
                f"'directory_path', 'id_column_name', 'content_column_names', 'timestamp_column_name'."
            )

        if not isinstance(self.content_column_names, list) or not self.content_column_names:
            raise ValueError(f"CSV connector '{self.config.name}': 'content_column_names' must be a non-empty list.")

        if self.metadata_column_names and not isinstance(self.metadata_column_names, list):
            logger.warning(f"CSV connector '{self.config.name}': 'metadata_column_names' is not a list. Will be ignored.")
            self.metadata_column_names = []

        logger.info(
            f"CsvDirExternalRepository '{self.config.name}' initialized. "
            f"Directory: {self.directory_path}, ID col: {self.id_column_name}, "
            f"Content cols: {self.content_column_names}, Timestamp col: {self.timestamp_column_name}"
        )

    async def connect(self) -> None:
        # For CSV directory, connect might just validate the directory path
        logger.info(f"CSV connector '{self.config.name}': 'connect' called. Verifying directory path.")
        if not os.path.isdir(self.directory_path):
            logger.error(f"CSV connector '{self.config.name}': Directory not found or not a directory: {self.directory_path}")
            raise BackendUnavailableError(f"CSV Directory '{self.directory_path}' not found for source '{self.config.name}'.")
        logger.info(f"CSV connector '{self.config.name}': Directory path '{self.directory_path}' verified.")

    async def disconnect(self) -> None:
        # No active connection to close for file system based repo
        logger.info(f"CSV connector '{self.config.name}': 'disconnect' called (no-op).")
        pass

    async def check_health(self) -> Tuple[bool, str]:
        if not os.path.isdir(self.directory_path):
            return False, f"Directory '{self.directory_path}' not found or is not a directory."
        if not os.access(self.directory_path, os.R_OK):
            return False, f"Directory '{self.directory_path}' not readable."
        return True, f"CSV Directory '{self.directory_path}' is accessible."

    async def retrieve_mapped_kems(
        self,
        internal_query: glm_service_pb2.KEMQuery,
        page_size: int,
        page_token: Optional[str] # Expected to be an integer offset as string for this connector
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        if not os.path.isdir(self.directory_path):
            raise BackendUnavailableError(f"CSV Directory '{self.directory_path}' not found for source '{self.config.name}'.")

        all_kems: List[kem_pb2.KEM] = []

        # Standard ISO 8601 format, but can be made configurable if needed
        # Common formats: '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%d %H:%M:%S%z'
        # For simplicity, try a few common ISO-like formats.
        # Python 3.7+ datetime.fromisoformat is good for strict ISO 8601.
        def parse_timestamp(ts_str: str, kem_id_for_log: str) -> Optional[datetime]:
            if not ts_str:
                return None
            try:
                # datetime.fromisoformat handles many ISO 8601 formats, including those with Z or +/-HH:MM
                # If it might have a space instead of T:
                dt_obj = datetime.fromisoformat(ts_str.replace(" ", "T"))
                # Ensure timezone-aware, default to UTC if naive
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                return dt_obj
            except ValueError:
                # Try common alternative formats if fromisoformat fails
                for fmt in ('%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z',
                            '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                    try:
                        dt_obj = datetime.strptime(ts_str, fmt)
                        if dt_obj.tzinfo is None: # Assume UTC if naive
                            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                        return dt_obj
                    except ValueError:
                        continue
                logger.warning(f"CSV Connector '{self.config.name}': Could not parse timestamp string '{ts_str}' for KEM ID '{kem_id_for_log}'.")
                return None

        try:
            csv_files = glob.glob(os.path.join(self.directory_path, "*.csv"))
            logger.info(f"CSV Connector '{self.config.name}': Found {len(csv_files)} CSV files in '{self.directory_path}'.")

            for csv_filepath in csv_files:
                logger.debug(f"CSV Connector '{self.config.name}': Processing file '{csv_filepath}'.")
                try:
                    with open(csv_filepath, mode='r', encoding=self.encoding, newline='') as file:
                        reader = csv.DictReader(file, delimiter=self.csv_delimiter)
                        for row_num, row in enumerate(reader):
                            kem_id_str = row.get(self.id_column_name)
                            if not kem_id_str:
                                logger.warning(f"CSV Connector '{self.config.name}': Missing ID in row {row_num+1} of file '{csv_filepath}'. Skipping.")
                                continue

                            # Content
                            content_parts = [row.get(col_name, "") for col_name in self.content_column_names]
                            full_content = " ".join(filter(None, content_parts)).strip()
                            if not full_content: # Skip if no content could be formed
                                logger.warning(f"CSV Connector '{self.config.name}': No content found for KEM ID '{kem_id_str}' in file '{csv_filepath}' using columns {self.content_column_names}. Skipping.")
                                continue

                            kem = kem_pb2.KEM()
                            kem.id = kem_id_str
                            kem.content = full_content.encode(self.encoding) # Assuming content is text
                            kem.content_type = self.content_type_value

                            # Timestamps
                            updated_at_str = row.get(self.timestamp_column_name)
                            updated_at_dt = parse_timestamp(updated_at_str, kem_id_str)
                            if updated_at_dt:
                                kem.updated_at.FromSeconds(int(updated_at_dt.timestamp()))
                                kem.updated_at.nanos = updated_at_dt.microsecond * 1000
                            else: # If primary sort timestamp is missing/unparseable, row is problematic for sorting
                                logger.warning(f"CSV Connector '{self.config.name}': Missing or unparseable sort timestamp '{self.timestamp_column_name}' for KEM ID '{kem_id_str}'. Skipping.")
                                continue

                            created_at_str = row.get(self.created_at_column_name, updated_at_str) # Default to updated_at if not specified
                            created_at_dt = parse_timestamp(created_at_str, kem_id_str)
                            if created_at_dt:
                                kem.created_at.FromSeconds(int(created_at_dt.timestamp()))
                                kem.created_at.nanos = created_at_dt.microsecond * 1000
                            else: # Use updated_at as created_at if created_at specific col is bad
                                kem.created_at.CopyFrom(kem.updated_at)


                            # Metadata
                            for meta_col_name in self.metadata_column_names:
                                meta_val = row.get(meta_col_name)
                                if meta_val is not None:
                                    kem.metadata[meta_col_name] = str(meta_val) # Store all as string

                            all_kems.append(kem)
                except FileNotFoundError:
                    logger.error(f"CSV Connector '{self.config.name}': File not found during processing: '{csv_filepath}'. This shouldn't happen if glob worked.")
                    continue # Skip this file
                except Exception as e_file:
                    logger.error(f"CSV Connector '{self.config.name}': Error processing file '{csv_filepath}': {e_file}", exc_info=True)
                    continue # Skip this file

            # In-memory Filtering based on internal_query.metadata_filters
            if internal_query.metadata_filters:
                filtered_kems = []
                for kem_obj in all_kems:
                    match = True
                    for filter_key, filter_value in internal_query.metadata_filters.items():
                        if kem_obj.metadata.get(filter_key) != filter_value:
                            match = False
                            break
                    if match:
                        filtered_kems.append(kem_obj)
                all_kems = filtered_kems
                logger.debug(f"CSV Connector '{self.config.name}': Filtered down to {len(all_kems)} KEMs after metadata filters.")

            # In-memory Sorting (updated_at DESC, id DESC)
            all_kems.sort(key=lambda k: (k.updated_at.seconds, k.updated_at.nanos, k.id), reverse=True)
            logger.debug(f"CSV Connector '{self.config.name}': Sorted {len(all_kems)} KEMs.")

            # In-memory Pagination
            current_offset = 0
            if page_token:
                try:
                    current_offset = int(page_token)
                except ValueError:
                    logger.warning(f"CSV Connector '{self.config.name}': Invalid page_token '{page_token}'. Defaulting to offset 0.")
                    current_offset = 0

            start_index = current_offset
            end_index = current_offset + page_size

            paginated_kems = all_kems[start_index:end_index]

            next_page_token_val: Optional[str] = None
            if end_index < len(all_kems):
                next_page_token_val = str(end_index)

            logger.info(f"CSV Connector '{self.config.name}': Returning {len(paginated_kems)} KEMs for page. Next token: {next_page_token_val}")
            return paginated_kems, next_page_token_val

        except Exception as e:
            logger.error(f"CSV Connector '{self.config.name}': Unexpected error in retrieve_mapped_kems: {e}", exc_info=True)
            raise StorageError(f"Failed to retrieve data from CSV source '{self.config.name}': {e}") from e
```
