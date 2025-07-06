import asyncio
import csv
import os
import glob
import logging
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone
import io

import aiofiles

from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2
from dcs_memory.services.glm.app.config import ExternalDataSourceConfig, GLMConfig # GLMConfig needed for type hints
from .external_base import BaseExternalRepository
from .base import StorageError, BackendUnavailableError, InvalidQueryError

logger = logging.getLogger(__name__)

class CsvDirExternalRepository(BaseExternalRepository):
    def __init__(self, config: ExternalDataSourceConfig, glm_config: GLMConfig):
        super().__init__(config, glm_config)
        mc = self.config.mapping_config
        self.directory_path: str = mc.get("directory_path")
        self.id_column_name: str = mc.get("id_column_name")
        self.content_column_names: List[str] = mc.get("content_column_names", [])
        self.timestamp_column_name: str = mc.get("timestamp_column_name")
        self.metadata_column_names: List[str] = mc.get("metadata_column_names", [])
        self.csv_delimiter: str = mc.get("csv_delimiter", ",")
        self.encoding: str = mc.get("encoding", "utf-8")
        self.content_type_value: str = mc.get("content_type_value", "text/plain")
        self.created_at_column_name: str = mc.get("created_at_column_name", self.timestamp_column_name)

        if not all([self.directory_path, self.id_column_name, self.content_column_names, self.timestamp_column_name]):
            raise ValueError(
                f"CSV connector '{self.config.name}': Missing required mapping_config: "
                f"'directory_path', 'id_column_name', 'content_column_names', 'timestamp_column_name'."
            )
        if not isinstance(self.content_column_names, list) or not self.content_column_names:
            raise ValueError(f"CSV connector '{self.config.name}': 'content_column_names' must be a non-empty list.")
        if self.metadata_column_names and not isinstance(self.metadata_column_names, list):
            logger.warning(f"CSV connector '{self.config.name}': 'metadata_column_names' not a list. Ignored.")
            self.metadata_column_names = []
        logger.info(f"CsvDirExternalRepository '{self.config.name}' initialized for dir: {self.directory_path}")

    async def connect(self) -> None:
        logger.info(f"CSV connector '{self.config.name}': Verifying directory path '{self.directory_path}'.")
        is_dir = await asyncio.to_thread(os.path.isdir, self.directory_path)
        if not is_dir:
            raise BackendUnavailableError(f"CSV Directory '{self.directory_path}' not found for '{self.config.name}'.")

    async def disconnect(self) -> None:
        logger.info(f"CSV connector '{self.config.name}': 'disconnect' (no-op).")
        pass

    async def check_health(self) -> Tuple[bool, str]:
        is_dir = await asyncio.to_thread(os.path.isdir, self.directory_path)
        if not is_dir: return False, f"Directory '{self.directory_path}' not found."
        is_readable = await asyncio.to_thread(os.access, self.directory_path, os.R_OK)
        if not is_readable: return False, f"Directory '{self.directory_path}' not readable."
        return True, f"CSV Directory '{self.directory_path}' accessible."

    def _parse_timestamp(self, ts_str: Optional[str], kem_id_for_log: str) -> Optional[datetime]:
        if not ts_str: return None
        try:
            dt_obj = datetime.fromisoformat(ts_str.replace(" ", "T"))
            return dt_obj.replace(tzinfo=timezone.utc) if dt_obj.tzinfo is None else dt_obj.astimezone(timezone.utc)
        except ValueError:
            for fmt in ('%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                try:
                    dt_obj = datetime.strptime(ts_str, fmt)
                    return dt_obj.replace(tzinfo=timezone.utc) if dt_obj.tzinfo is None else dt_obj.astimezone(timezone.utc)
                except ValueError: continue
            logger.warning(f"CSV Connector '{self.config.name}': Could not parse ts '{ts_str}' for KEM ID '{kem_id_for_log}'.")
            return None

    def _process_row_to_kem(self, row: Dict[str, str], csv_filepath: str, row_num: int) -> Optional[kem_pb2.KEM]:
        kem_id_str = row.get(self.id_column_name)
        if not kem_id_str:
            logger.warning(f"CSV '{self.config.name}': Missing ID in row {row_num} of '{csv_filepath}'. Skipping.")
            return None

        content_parts = [row.get(col_name, "") for col_name in self.content_column_names]
        full_content = " ".join(filter(None, content_parts)).strip()
        if not full_content and self.content_column_names:
            logger.warning(f"CSV '{self.config.name}': No content for KEM ID '{kem_id_str}' in '{csv_filepath}'. Skipping.")
            return None

        kem = kem_pb2.KEM(id=kem_id_str, content_type=self.content_type_value)
        try:
            kem.content = full_content.encode(self.encoding)
        except UnicodeEncodeError:
            logger.error(f"CSV '{self.config.name}': Unicode encode error for KEM ID '{kem_id_str}'. Skipping.")
            return None


        updated_at_dt = self._parse_timestamp(row.get(self.timestamp_column_name), kem_id_str)
        if not updated_at_dt:
            logger.warning(f"CSV '{self.config.name}': Missing/unparseable sort timestamp for KEM ID '{kem_id_str}'. Skipping.")
            return None
        kem.updated_at.FromSeconds(int(updated_at_dt.timestamp())); kem.updated_at.nanos = updated_at_dt.microsecond * 1000

        created_at_dt = self._parse_timestamp(row.get(self.created_at_column_name, row.get(self.timestamp_column_name)), kem_id_str)
        if created_at_dt:
            kem.created_at.FromSeconds(int(created_at_dt.timestamp())); kem.created_at.nanos = created_at_dt.microsecond * 1000
        else:
            kem.created_at.CopyFrom(kem.updated_at)

        for meta_col_name in self.metadata_column_names:
            meta_val = row.get(meta_col_name)
            if meta_val is not None: kem.metadata[meta_col_name] = str(meta_val)

        return kem

    async def _read_and_parse_one_csv_file(self, csv_filepath: str, internal_query: glm_service_pb2.KEMQuery) -> List[kem_pb2.KEM]:
        kems_in_file: List[kem_pb2.KEM] = []
        query_ids_set = set(internal_query.ids) if internal_query.ids else None

        # Date range filters from KEMQuery (converted to datetime once)
        created_at_start_dt = self._parse_timestamp(internal_query.created_at_start.ToJsonString(), "query.created_at_start") if internal_query.HasField("created_at_start") else None
        created_at_end_dt = self._parse_timestamp(internal_query.created_at_end.ToJsonString(), "query.created_at_end") if internal_query.HasField("created_at_end") else None
        updated_at_start_dt = self._parse_timestamp(internal_query.updated_at_start.ToJsonString(), "query.updated_at_start") if internal_query.HasField("updated_at_start") else None
        updated_at_end_dt = self._parse_timestamp(internal_query.updated_at_end.ToJsonString(), "query.updated_at_end") if internal_query.HasField("updated_at_end") else None

        try:
            async with aiofiles.open(csv_filepath, mode='r', encoding=self.encoding, newline='') as afp:
                file_content = await afp.read()

            # Process content in a way that doesn't block the event loop for too long if parsing is CPU intensive
            # For standard CSV, DictReader on StringIO is usually acceptable unless files are truly massive AND have many columns.
            # If parsing becomes a bottleneck, this part might need `asyncio.to_thread` for the `csv.DictReader` loop.
            string_io_buffer = io.StringIO(file_content)
            reader = csv.DictReader(string_io_buffer, delimiter=self.csv_delimiter)

            for row_num, row_dict in enumerate(reader):
                kem = self._process_row_to_kem(row_dict, csv_filepath, row_num + 1)
                if not kem: continue

                # Apply ID filter (if any)
                if query_ids_set and kem.id not in query_ids_set: continue

                # Apply metadata filters (if any)
                if internal_query.metadata_filters:
                    match = True
                    for f_key, f_val in internal_query.metadata_filters.items():
                        if kem.metadata.get(f_key) != f_val: match = False; break
                    if not match: continue

                # Apply date range filters
                kem_created_at_dt = datetime.fromtimestamp(kem.created_at.seconds + kem.created_at.nanos / 1e9, tz=timezone.utc)
                kem_updated_at_dt = datetime.fromtimestamp(kem.updated_at.seconds + kem.updated_at.nanos / 1e9, tz=timezone.utc)

                if created_at_start_dt and kem_created_at_dt < created_at_start_dt: continue
                if created_at_end_dt and kem_created_at_dt > created_at_end_dt: continue
                if updated_at_start_dt and kem_updated_at_dt < updated_at_start_dt: continue
                if updated_at_end_dt and kem_updated_at_dt > updated_at_end_dt: continue

                kems_in_file.append(kem)
        except FileNotFoundError:
            logger.error(f"CSV Connector '{self.config.name}': File not found: '{csv_filepath}'.")
        except Exception as e_file:
            logger.error(f"CSV Connector '{self.config.name}': Error processing file '{csv_filepath}': {e_file}", exc_info=True)
        return kems_in_file

    async def retrieve_mapped_kems(
        self, internal_query: glm_service_pb2.KEMQuery, page_size: int, page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:

        is_dir = await asyncio.to_thread(os.path.isdir, self.directory_path)
        if not is_dir:
            raise BackendUnavailableError(f"CSV Directory '{self.directory_path}' not found for source '{self.config.name}'.")

        all_kems: List[kem_pb2.KEM] = []
        try:
            path_pattern = os.path.join(self.directory_path, "*.csv")
            csv_files = await asyncio.to_thread(glob.glob, path_pattern)
            logger.info(f"CSV Connector '{self.config.name}': Found {len(csv_files)} CSV files.")

            file_processing_tasks = [self._read_and_parse_one_csv_file(fp, internal_query) for fp in csv_files]
            results_from_files = await asyncio.gather(*file_processing_tasks, return_exceptions=True)

            for result in results_from_files:
                if isinstance(result, Exception):
                    logger.error(f"CSV Connector '{self.config.name}': Error in a file processing task: {result}", exc_info=result)
                elif isinstance(result, list):
                    all_kems.extend(result)

            logger.info(f"CSV Connector '{self.config.name}': Loaded and filtered {len(all_kems)} KEMs from all files.")

            # In-memory Sorting (run in thread if dataset is large)
            def sort_kems_sync(kems_to_sort):
                # Sort by updated_at (desc), then id (desc) for stable pagination
                kems_to_sort.sort(key=lambda k: (k.updated_at.seconds, k.updated_at.nanos, k.id), reverse=True)
                return kems_to_sort

            if all_kems: # Only sort if there's anything to sort
                all_kems = await asyncio.to_thread(sort_kems_sync, all_kems)
            logger.debug(f"CSV Connector '{self.config.name}': Sorted {len(all_kems)} KEMs.")

            current_offset = 0
            if page_token:
                try: current_offset = int(page_token)
                except ValueError: logger.warning(f"CSV Connector '{self.config.name}': Invalid page_token. Defaulting to 0.")

            start_index = current_offset
            end_index = current_offset + page_size
            paginated_kems = all_kems[start_index:end_index]
            next_page_token_val: Optional[str] = str(end_index) if end_index < len(all_kems) else None

            logger.info(f"CSV Connector '{self.config.name}': Returning {len(paginated_kems)} KEMs. Next token: {next_page_token_val}")
            return paginated_kems, next_page_token_val
        except Exception as e:
            logger.error(f"CSV Connector '{self.config.name}': Unexpected error in retrieve_mapped_kems: {e}", exc_info=True)
            raise StorageError(f"Failed to retrieve data from CSV source '{self.config.name}': {e}") from e

```
