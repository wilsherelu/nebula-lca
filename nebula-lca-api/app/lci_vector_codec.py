"""Packing helpers for compressed ecoinvent LCI inventory vectors."""

from __future__ import annotations

from array import array
from dataclasses import dataclass
import hashlib
import zlib


@dataclass(frozen=True)
class PackedLciVector:
    flow_key_ids_blob: bytes
    amounts_blob: bytes
    nnz: int
    checksum: str
    compression: str = "zlib"
    index_dtype: str = "uint32"
    amount_dtype: str = "float64"


def _pack_uint32(values: list[int]) -> bytes:
    arr = array("I", values)
    if arr.itemsize != 4:
        raise RuntimeError("array('I') is not 32-bit on this Python runtime")
    return arr.tobytes()


def _unpack_uint32(raw: bytes) -> list[int]:
    arr = array("I")
    if arr.itemsize != 4:
        raise RuntimeError("array('I') is not 32-bit on this Python runtime")
    arr.frombytes(raw)
    return list(arr)


def _pack_float64(values: list[float]) -> bytes:
    arr = array("d", values)
    if arr.itemsize != 8:
        raise RuntimeError("array('d') is not 64-bit on this Python runtime")
    return arr.tobytes()


def _unpack_float64(raw: bytes) -> list[float]:
    arr = array("d")
    if arr.itemsize != 8:
        raise RuntimeError("array('d') is not 64-bit on this Python runtime")
    arr.frombytes(raw)
    return list(arr)


def pack_lci_vector(
    flow_key_ids: list[int],
    amounts: list[float],
    *,
    compression_level: int = 1,
) -> PackedLciVector:
    if len(flow_key_ids) != len(amounts):
        raise ValueError("flow_key_ids and amounts must have the same length")
    if flow_key_ids != sorted(flow_key_ids):
        raise ValueError("flow_key_ids must be sorted before packing")

    index_raw = _pack_uint32(flow_key_ids)
    amount_raw = _pack_float64(amounts)
    checksum = hashlib.sha256(index_raw + amount_raw).hexdigest()
    return PackedLciVector(
        flow_key_ids_blob=zlib.compress(index_raw, level=compression_level),
        amounts_blob=zlib.compress(amount_raw, level=compression_level),
        nnz=len(flow_key_ids),
        checksum=checksum,
    )


def unpack_lci_vector(
    *,
    flow_key_ids_blob: bytes,
    amounts_blob: bytes,
    nnz: int,
    compression: str = "zlib",
) -> tuple[list[int], list[float]]:
    if compression != "zlib":
        raise ValueError(f"Unsupported LCI vector compression: {compression}")
    flow_key_ids = _unpack_uint32(zlib.decompress(flow_key_ids_blob))
    amounts = _unpack_float64(zlib.decompress(amounts_blob))
    if len(flow_key_ids) != nnz or len(amounts) != nnz:
        raise ValueError("LCI vector blob length does not match nnz")
    return flow_key_ids, amounts


def unpack_lci_flow_key_ids(*, flow_key_ids_blob: bytes, nnz: int, compression: str = "zlib") -> list[int]:
    if compression != "zlib":
        raise ValueError(f"Unsupported LCI vector compression: {compression}")
    flow_key_ids = _unpack_uint32(zlib.decompress(flow_key_ids_blob))
    if len(flow_key_ids) != nnz:
        raise ValueError("LCI flow-key axis blob length does not match nnz")
    return flow_key_ids


def checksum_lci_vector(flow_key_ids: list[int], amounts: list[float]) -> str:
    if len(flow_key_ids) != len(amounts):
        raise ValueError("flow_key_ids and amounts must have the same length")
    return hashlib.sha256(_pack_uint32(flow_key_ids) + _pack_float64(amounts)).hexdigest()
