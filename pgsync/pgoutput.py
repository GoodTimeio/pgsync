"""Minimal decoder for the PostgreSQL ``pgoutput`` logical replication stream.

``test_decoding`` emits a human readable text stream for *every* table in the
database and cannot be filtered.  ``pgoutput`` instead only emits changes for
the tables contained in a publication, which lets a slot be scoped to just the
tables an index needs.

This module implements the subset of the logical replication message protocol
(version 1) that pgsync requires: ``Begin``, ``Commit``, ``Relation``,
``Insert``, ``Update``, ``Delete`` and ``Truncate``.  Higher protocol versions
only add streaming of in-progress transactions, which pgsync does not use.

Reference:
https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

import struct
import typing as t
from dataclasses import dataclass

from .constants import DELETE, INSERT, TRUNCATE, UPDATE

# Message tags
_BEGIN = b"B"
_COMMIT = b"C"
_ORIGIN = b"O"
_RELATION = b"R"
_TYPE = b"Y"
_INSERT = b"I"
_UPDATE = b"U"
_DELETE = b"D"
_TRUNCATE = b"T"
_MESSAGE = b"M"

# TupleData column kinds
_NULL = ord("n")  # value is NULL
_TOAST = ord("u")  # unchanged TOAST value (not sent)
_TEXT = ord("t")  # value is text encoded
_BINARY = ord("b")  # value is binary encoded


@dataclass
class Relation:
    relid: int
    namespace: str
    name: str
    columns: t.List[str]


class Change(t.NamedTuple):
    tg_op: str
    schema: str
    table: str
    old: t.Optional[dict]
    new: t.Optional[dict]


class _Reader:
    """Sequential big-endian reader over a pgoutput message body."""

    __slots__ = ("data", "pos")

    def __init__(self, data: bytes) -> None:
        self.data: bytes = data
        self.pos: int = 0

    def int8(self) -> int:
        value: int = self.data[self.pos]
        self.pos += 1
        return value

    def char(self) -> bytes:
        value: bytes = self.data[self.pos : self.pos + 1]
        self.pos += 1
        return value

    def int16(self) -> int:
        (value,) = struct.unpack_from("!h", self.data, self.pos)
        self.pos += 2
        return value

    def int32(self) -> int:
        (value,) = struct.unpack_from("!i", self.data, self.pos)
        self.pos += 4
        return value

    def int64(self) -> int:
        (value,) = struct.unpack_from("!q", self.data, self.pos)
        self.pos += 8
        return value

    def string(self) -> str:
        end: int = self.data.index(b"\x00", self.pos)
        value: str = self.data[self.pos : end].decode("utf-8")
        self.pos = end + 1
        return value

    def raw(self, length: int) -> bytes:
        value: bytes = self.data[self.pos : self.pos + length]
        self.pos += length
        return value


class PgOutputDecoder:
    """Stateful decoder that turns pgoutput messages into pgsync payloads.

    A single instance must be reused for the lifetime of a replication stream
    because ``Relation`` messages describe the column layout used by subsequent
    ``Insert``/``Update``/``Delete`` messages.

    NB: column values are decoded as text (their ``::text`` representation).
    This is sufficient for pgsync because downstream resolution only relies on
    primary-key and foreign-key columns, which are re-materialised from the
    source database. Primary keys are never stored as unchanged-TOAST values.
    """

    def __init__(self) -> None:
        self._relations: t.Dict[int, Relation] = {}

    def decode(self, data: bytes) -> t.Optional[t.Any]:
        """Decode a single message.

        Returns:
            - ``("begin", None)``
            - ``("commit", end_lsn:int)``
            - ``("relation", Relation)``
            - ``("change", Change)``
            - ``("truncate", List[Change])``
            - ``None`` for messages pgsync ignores.
        """
        if not data:
            return None
        tag: bytes = data[0:1]

        if tag == _INSERT:
            return ("change", self._decode_insert(data))
        if tag == _UPDATE:
            return ("change", self._decode_update(data))
        if tag == _DELETE:
            return ("change", self._decode_delete(data))
        if tag == _RELATION:
            return ("relation", self._decode_relation(data))
        if tag == _COMMIT:
            return ("commit", self._decode_commit(data))
        if tag == _BEGIN:
            return ("begin", None)
        if tag == _TRUNCATE:
            return ("truncate", self._decode_truncate(data))
        # Type, Origin, Message and anything else are not needed.
        return None

    # -- individual message decoders ------------------------------------

    def _decode_relation(self, data: bytes) -> Relation:
        reader: _Reader = _Reader(data)
        reader.char()  # tag
        relid: int = reader.int32()
        namespace: str = reader.string()
        name: str = reader.string()
        reader.int8()  # replica identity setting
        ncolumns: int = reader.int16()
        columns: t.List[str] = []
        for _ in range(ncolumns):
            reader.int8()  # column flags (1 => part of key)
            columns.append(reader.string())
            reader.int32()  # type oid
            reader.int32()  # type modifier
        relation: Relation = Relation(relid, namespace, name, columns)
        self._relations[relid] = relation
        return relation

    def _decode_commit(self, data: bytes) -> int:
        reader: _Reader = _Reader(data)
        reader.char()  # tag
        reader.int8()  # flags
        reader.int64()  # commit LSN
        end_lsn: int = reader.int64()  # end LSN (transaction end)
        reader.int64()  # commit timestamp
        return end_lsn

    def _decode_truncate(self, data: bytes) -> t.List[Change]:
        reader: _Reader = _Reader(data)
        reader.char()  # tag
        nrelations: int = reader.int32()
        reader.int8()  # option bits (CASCADE / RESTART IDENTITY)
        changes: t.List[Change] = []
        for _ in range(nrelations):
            relid: int = reader.int32()
            relation: t.Optional[Relation] = self._relations.get(relid)
            if relation is not None:
                changes.append(
                    Change(
                        tg_op=TRUNCATE,
                        schema=relation.namespace,
                        table=relation.name,
                        old=None,
                        new=None,
                    )
                )
        return changes

    def _decode_insert(self, data: bytes) -> Change:
        reader: _Reader = _Reader(data)
        reader.char()  # tag
        relid: int = reader.int32()
        reader.char()  # 'N'
        relation: Relation = self._relations[relid]
        new: dict = self._read_tuple(reader, relation)
        return Change(
            tg_op=INSERT,
            schema=relation.namespace,
            table=relation.name,
            old=None,
            new=new,
        )

    def _decode_update(self, data: bytes) -> Change:
        reader: _Reader = _Reader(data)
        reader.char()  # tag
        relid: int = reader.int32()
        relation: Relation = self._relations[relid]
        old: t.Optional[dict] = None
        kind: bytes = reader.char()
        if kind in (b"K", b"O"):
            # old tuple identified by key (K) or full old row (O)
            old = self._read_tuple(reader, relation)
            kind = reader.char()  # should now be 'N'
        # kind == b"N" -> new tuple follows
        new: dict = self._read_tuple(reader, relation)
        return Change(
            tg_op=UPDATE,
            schema=relation.namespace,
            table=relation.name,
            old=old,
            new=new,
        )

    def _decode_delete(self, data: bytes) -> Change:
        reader: _Reader = _Reader(data)
        reader.char()  # tag
        relid: int = reader.int32()
        relation: Relation = self._relations[relid]
        reader.char()  # 'K' (key) or 'O' (old)
        old: dict = self._read_tuple(reader, relation)
        return Change(
            tg_op=DELETE,
            schema=relation.namespace,
            table=relation.name,
            old=old,
            new=None,
        )

    def _read_tuple(self, reader: _Reader, relation: Relation) -> dict:
        ncolumns: int = reader.int16()
        row: dict = {}
        for index in range(ncolumns):
            kind: int = reader.int8()
            name: str = (
                relation.columns[index]
                if index < len(relation.columns)
                else str(index)
            )
            if kind == _NULL:
                row[name] = None
            elif kind == _TOAST:
                # unchanged TOAST value; not transmitted -> omit entirely
                continue
            elif kind in (_TEXT, _BINARY):
                length: int = reader.int32()
                value: bytes = reader.raw(length)
                row[name] = value.decode("utf-8", errors="replace")
            else:
                # unknown kind; be defensive and skip
                continue
        return row
