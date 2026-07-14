"""Data contracts for lossless elementary-flow conversion.

The contracts deliberately keep source identifiers and target identifiers in
separate namespaces. Cross-database equality is expressed only by reviewed
directional mappings.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CfPresence(str, Enum):
    MISSING = "missing"
    ZERO = "zero"
    NONZERO = "nonzero"


class SemanticStatus(str, Enum):
    EXACT = "exact"
    COMPATIBLE = "compatible"
    CONFLICT = "conflict"
    UNKNOWN = "unknown"


class MappingGrade(str, Enum):
    S1 = "S1"
    S2 = "S2"
    S3 = "S3"
    S4 = "S4"
    S5 = "S5"


class MappingStatus(str, Enum):
    MAPPED = "mapped"
    RESIDUAL = "residual"
    FORBIDDEN = "forbidden"


@dataclass(frozen=True, order=True)
class FlowIdentity:
    """Normalized identity used by the certification layer.

    ``substance_id`` and ``context_id`` refer to reviewed canonical records;
    they are not derived from cross-namespace UUID equality.
    """

    namespace: str
    namespace_version: str
    flow_id: str
    substance_id: str
    context_id: str
    direction: str
    flow_property: str
    unit_dimension: str
    qualifiers: tuple[tuple[str, str], ...] = ()

    @property
    def key(self) -> str:
        return f"{self.namespace}:{self.namespace_version}:{self.flow_id}"

    @property
    def semantic_key(self) -> tuple[object, ...]:
        return (
            self.substance_id,
            self.context_id,
            self.direction,
            self.flow_property,
            self.unit_dimension,
            self.qualifiers,
        )


@dataclass(frozen=True)
class CfValue:
    presence: CfPresence
    value: float | None = None

    def __post_init__(self) -> None:
        if self.presence == CfPresence.MISSING and self.value is not None:
            raise ValueError("missing CF must not carry a numeric value")
        if self.presence == CfPresence.ZERO and self.value != 0.0:
            raise ValueError("zero CF must carry value 0.0")
        if self.presence == CfPresence.NONZERO and (self.value is None or self.value == 0.0):
            raise ValueError("nonzero CF must carry a non-zero numeric value")


@dataclass(frozen=True)
class MethodScope:
    method_id: str
    method_version: str
    indicator_ids: tuple[str, ...]
    relative_tolerance: float = 1e-8
    absolute_tolerance: float = 1e-15

    def __post_init__(self) -> None:
        if not self.indicator_ids:
            raise ValueError("method scope requires at least one indicator")
        if self.relative_tolerance < 0 or self.absolute_tolerance < 0:
            raise ValueError("tolerances must be non-negative")


@dataclass(frozen=True)
class DirectionalMapping:
    source: FlowIdentity
    target: FlowIdentity
    unit_factor: float
    basis_factor: float = 1.0
    semantic_status: SemanticStatus = SemanticStatus.UNKNOWN
    grade: MappingGrade = MappingGrade.S4
    review_status: str = "draft"
    evidence_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.source.namespace == self.target.namespace:
            raise ValueError("cross-namespace mapping requires different namespaces")
        if self.unit_factor <= 0 or self.basis_factor <= 0:
            raise ValueError("mapping factors must be positive")

    @property
    def amount_factor(self) -> float:
        return self.unit_factor * self.basis_factor


@dataclass(frozen=True)
class RejectedMapping:
    source_key: str
    target_key: str
    codes: tuple[str, ...]


@dataclass(frozen=True)
class ConversionPackage:
    package_id: str
    package_version: str
    method_scope: MethodScope
    forward_mappings: tuple[DirectionalMapping, ...]
    reverse_mappings: tuple[DirectionalMapping, ...]
    package_hash: str
    one_way_mappings: tuple[DirectionalMapping, ...] = ()


@dataclass(frozen=True)
class CompilationResult:
    package: ConversionPackage
    rejected: tuple[RejectedMapping, ...] = ()


@dataclass(frozen=True)
class InventoryExchange:
    flow: FlowIdentity
    amount: float

    def __post_init__(self) -> None:
        if self.amount < 0:
            raise ValueError("inventory amounts must be non-negative; direction is explicit")


@dataclass(frozen=True)
class ConversionTrace:
    source: InventoryExchange
    status: MappingStatus
    target: InventoryExchange | None = None
    mapping_evidence_ids: tuple[str, ...] = ()
    warning_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConversionResult:
    converted: tuple[InventoryExchange, ...]
    residual: tuple[InventoryExchange, ...]
    traces: tuple[ConversionTrace, ...]
    package_hash: str
