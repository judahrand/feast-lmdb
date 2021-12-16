import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import lmdb
import pytz
from feast.infra.key_encoding_utils import (serialize_entity_key,
                                            serialize_entity_key_prefix)
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureView, RepoConfig

logger = logging.getLogger(__name__)


class LMDBOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the LMBD online store.
    """

    type: Literal[
        "lmdb", "feast_lmdb.lmdb_online_store.LMDBOnlineStore"
    ] = "feast_lmdb.lmdb_online_store.LMDBOnlineStore"

    path: StrictStr = "data/online/"
    """ (optional) Path to LMDB environment."""


class LMDBOnlineStore(OnlineStore):
    """
    An online store implementation that uses LMDB.
    """

    _conn: lmdb.Environment = None

    def _get_conn(self, config: RepoConfig):

        online_store_config = config.online_store
        assert isinstance(online_store_config, LMDBOnlineStoreConfig)

        Path(online_store_config.path).mkdir(parents=True, exist_ok=True)
        if not self._conn:
            self._conn = lmdb.open(online_store_config.path)
        return self._conn

    def delete_table_values(self, config: RepoConfig, table: FeatureView):
        conn = self._get_conn(config)
        deleted_count = 0
        prefix = _lmdb_table_prefix(config.project, table)

        with conn.begin(write=True) as txn:
            with txn.cursor() as cur:
                cur.set_range(prefix)
                cur.next()
                while cur.key().startswith(prefix) is True:
                    cur.delete()
                    deleted_count += 1

        # Without this the database file only ever grows.
        with tempfile.TemporaryDirectory(prefix="feast-lmdb-") as tmpdir:
            conn.copy(tmpdir, compact=True)
            for file in Path(tmpdir).iterdir():
                file.replace(Path(config.online_store.path, file.name))

        logger.debug(f"Deleted {deleted_count} keys for {table.name}")

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        conn = self._get_conn(config)

        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = _lmdb_entity_key(config.project, table, entity_key)
            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            with conn.begin(write=True, buffers=True) as txn:
                with txn.cursor() as cur:
                    # If record already exists for entity_key and is newer than
                    # current record then do not update.
                    val = cur.get(entity_key_bin + ":_ts".encode())
                    if val:
                        prev_ts = Timestamp()
                        prev_ts.ParseFromString(val)
                        if (
                            prev_ts.seconds
                            and int(timestamp.timestamp()) <= prev_ts.seconds
                        ):
                            if progress:
                                progress(1)
                            continue

                    records = {}
                    records[entity_key_bin + ":_ts".encode()] = Timestamp(
                        seconds=int(timestamp.timestamp())
                    ).SerializeToString()
                    if created_ts:
                        records[entity_key_bin + ":_created".encode()] = Timestamp(
                            seconds=int(created_ts.timestamp())
                        ).SerializeToString()
                    for feature_name, val in values.items():
                        records[
                            entity_key_bin + (":" + feature_name).encode()
                        ] = val.SerializeToString()

                    cur.putmulti(records.items())
                    if progress:
                        progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        conn = self._get_conn(config)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        project = config.project

        ts_key = "_ts"
        if requested_features is None:
            requested_features = [feature.name for feature in table.features]

        for entity_key in entity_keys:
            entity_key_bin = _lmdb_entity_key(project, table, entity_key)
            keys = [
                entity_key_bin + (":" + feature).encode()
                for feature in requested_features
            ]
            keys.append(entity_key_bin + (":" + ts_key).encode())

            with conn.begin() as txn:
                with txn.cursor() as cur:
                    res = cur.getmulti(keys)

            res = {k.split(b":")[-1].decode(): v for k, v in res}
            if not res:
                # If no keys found.
                result.append((None, None))
                continue

            res_ts = None
            if ts_key in res:
                val = Timestamp()
                val.ParseFromString(res[ts_key])
                res_ts = datetime.fromtimestamp(val.seconds)
                del res[ts_key]

            for key in requested_features:
                val = ValueProto()
                if key in res:
                    val.ParseFromString(res[key])
                res[key] = val

            result.append((res_ts, res))
        return result

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        for table in tables_to_delete:
            self.delete_table_values(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        for table in tables:
            self.delete_table_values(config, table)


def to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _lmdb_table_prefix(project: str, table: FeatureView) -> bytes:
    return _table_id(project, table).encode("utf-8")


def _lmdb_entity_key(
    project: str, table: FeatureView, entity_key: EntityKeyProto
) -> bytes:
    key: List[bytes] = [
        _lmdb_table_prefix(project, table),
        serialize_entity_key(entity_key),
    ]
    return b"".join(key)


def _lmdb_entity_key_prefix(
    project: str, table: FeatureView, entity_keys: List[str]
) -> bytes:
    key: List[bytes] = [
        _lmdb_table_prefix(project, table),
        serialize_entity_key_prefix(entity_keys),
    ]
    return b"".join(key)
