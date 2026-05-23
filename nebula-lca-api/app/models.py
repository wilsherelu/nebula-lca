from datetime import datetime
import uuid
from sqlalchemy import String, Integer, DateTime, ForeignKey, Text, Float, Boolean, UniqueConstraint, LargeBinary
from sqlalchemy import JSON as SAJSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from .database import Base

JsonType = SAJSON().with_variant(JSONB, "postgresql")


class ReferenceProcess(Base):
    __tablename__ = "reference_processes"

    process_uuid: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_name: Mapped[str] = mapped_column(String(255), nullable=False)
    process_name_zh: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    process_name_en: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    process_type: Mapped[str] = mapped_column(String(64), nullable=False, default="unit_process", index=True)
    reference_flow_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    reference_flow_internal_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    process_json: Mapped[dict | None] = mapped_column(JsonType, nullable=True)
    source_file: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    source_process_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    import_mode: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    import_report_json: Mapped[dict | None] = mapped_column(JsonType, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class FlowRecord(Base):
    __tablename__ = "flow_catalog"

    flow_uuid: Mapped[str] = mapped_column(String(64), primary_key=True)
    flow_name: Mapped[str] = mapped_column(String(255), nullable=False)
    flow_name_en: Mapped[str | None] = mapped_column(String(255), nullable=True)
    flow_type: Mapped[str] = mapped_column(String(64), nullable=False)
    default_unit: Mapped[str] = mapped_column(String(64), nullable=False)
    unit_group: Mapped[str] = mapped_column(String(64), nullable=False)
    compartment: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_updated_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Stage 1 custom-flow metadata — columns added at startup via _ensure_custom_flow_columns()
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_custom: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tidas_compatible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tidas_unit_group: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tidas_flow_property_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tidas_reference_source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    allocation_properties: Mapped[list | None] = mapped_column(JsonType, nullable=True)


class UnitGroup(Base):
    __tablename__ = "unit_groups"

    name: Mapped[str] = mapped_column(String(128), primary_key=True)
    reference_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_package_version: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_file: Mapped[str | None] = mapped_column(String(1024), nullable=True)


class UnitDefinition(Base):
    __tablename__ = "unit_definitions"
    __table_args__ = (UniqueConstraint("unit_group", "unit_name", name="uq_unit_group_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    unit_group: Mapped[str] = mapped_column(String(128), ForeignKey("unit_groups.name"), nullable=False, index=True)
    unit_name: Mapped[str] = mapped_column(String(64), nullable=False)
    factor_to_reference: Mapped[float] = mapped_column(Float, nullable=False)
    is_reference: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class Model(Base):
    __tablename__ = "models"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    reference_product: Mapped[str | None] = mapped_column(Text, nullable=True)
    functional_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    system_boundary: Mapped[str | None] = mapped_column(Text, nullable=True)
    time_representativeness: Mapped[str | None] = mapped_column(Text, nullable=True)
    geography: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Source policy: which source-system compliance mode this project uses.
    # Defaults to "open_mixed" for backward compatibility with existing projects.
    source_policy: Mapped[str] = mapped_column(String(32), nullable=False, default="open_mixed", index=True)
    # Allowed LCIA scope: which LCIA runtimes may be used.
    allowed_lcia_scope: Mapped[str] = mapped_column(String(32), nullable=False, default="ef31_only", index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class ModelVersion(Base):
    __tablename__ = "model_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    model_id: Mapped[str] = mapped_column(String(36), ForeignKey("models.id"), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    graph_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    hybrid_graph_json: Mapped[dict] = mapped_column(JsonType, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class RunJob(Base):
    __tablename__ = "run_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    model_version_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("model_versions.id"), nullable=True, index=True
    )
    status: Mapped[str] = mapped_column(String(32), default="completed", nullable=False)
    request_json: Mapped[dict] = mapped_column(JsonType, nullable=False)
    result_json: Mapped[dict] = mapped_column(JsonType, nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class PtsCompileArtifact(Base):
    __tablename__ = "pts_compile_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "pts_node_id",
            "graph_hash",
            name="uq_pts_compile_project_node_hash",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    graph_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    compile_version: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    ok: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    matrix_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invertible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    errors_json: Mapped[list] = mapped_column(JsonType, nullable=False, default=list)
    warnings_json: Mapped[list] = mapped_column(JsonType, nullable=False, default=list)
    artifact_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class PtsDefinition(Base):
    __tablename__ = "pts_definitions"
    __table_args__ = (UniqueConstraint("project_id", "pts_uuid", name="uq_pts_definition_project_pts"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    pts_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    internal_node_ids_json: Mapped[list] = mapped_column(JsonType, nullable=False, default=list)
    product_refs_json: Mapped[list] = mapped_column(JsonType, nullable=False, default=list)
    ports_policy_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    latest_graph_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    definition_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class PtsResource(Base):
    __tablename__ = "pts_resources"
    __table_args__ = (UniqueConstraint("pts_uuid", name="uq_pts_resource_pts_uuid"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    pts_node_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    latest_graph_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    compiled_graph_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    latest_compile_version: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    latest_published_version: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    active_published_version: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    pts_graph_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    ports_policy_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    shell_node_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class PtsExternalArtifact(Base):
    __tablename__ = "pts_external_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "pts_uuid",
            "graph_hash",
            name="uq_pts_external_project_pts_hash",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    pts_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pts_node_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    graph_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    published_version: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    source_compile_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    source_compile_version: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    artifact_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class DebugDiagnostic(Base):
    __tablename__ = "debug_diagnostics"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    diagnostic_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    project_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    graph_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    payload_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    result_json: Mapped[dict] = mapped_column(JsonType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class LciExchangeMatrix(Base):
    """Sparse matrix storing elementary inventory for ecoinvent LCI processes.

    Each row represents one non-zero elementary exchange within a process.
    Multiple units for the same process/flow/direction must stay separate until
    a later unit-normalization step can safely combine them.
    """

    __tablename__ = "lci_exchange_matrix"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    process_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    flow_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str] = mapped_column(String(64), nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_package_version: Mapped[str | None] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        UniqueConstraint("process_uuid", "flow_uuid", "direction", "unit", name="uq_lci_process_flow_direction_unit"),
    )


class LciBiosphereFlowKey(Base):
    """Dictionary key for canonicalized elementary inventory vectors."""

    __tablename__ = "lci_biosphere_flow_keys"

    flow_key_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_uuid: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    compartment: Mapped[str | None] = mapped_column(String(255), nullable=True)
    subcompartment: Mapped[str | None] = mapped_column(String(255), nullable=True)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    canonical_unit: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_package_version: Mapped[str | None] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "flow_uuid",
            "compartment",
            "subcompartment",
            "direction",
            "canonical_unit",
            name="uq_lci_flow_key_identity",
        ),
    )


class LciVectorAxis(Base):
    """Reusable sorted flow-key axis for compressed LCI vectors."""

    __tablename__ = "lci_vector_axes"

    axis_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nnz: Mapped[int] = mapped_column(Integer, nullable=False)
    flow_key_ids_blob: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    compression: Mapped[str] = mapped_column(String(32), nullable=False, default="zlib")


class LciProcessVector(Base):
    """Compressed elementary inventory vector for one linked LCI process."""

    __tablename__ = "lci_process_vectors"

    process_uuid: Mapped[str] = mapped_column(String(64), primary_key=True)
    dataset_level: Mapped[str] = mapped_column(String(32), nullable=False, default="linked_lci")
    system_model: Mapped[str | None] = mapped_column(String(64), nullable=True)
    nnz: Mapped[int] = mapped_column(Integer, nullable=False)
    axis_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    flow_key_ids_blob: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    amounts_blob: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    index_dtype: Mapped[str] = mapped_column(String(32), nullable=False, default="uint32")
    amount_dtype: Mapped[str] = mapped_column(String(32), nullable=False, default="float64")
    compression: Mapped[str] = mapped_column(String(32), nullable=False, default="zlib")
    canonicalized: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_package_version: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


# ======================================================================
# Ecoinvent Import Task System
# ======================================================================


class ImportJob(Base):
    """Persistent import task for EF 3.1 LCI/LCIA ingestion."""

    __tablename__ = "import_jobs"

    job_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    file_path: Mapped[str] = mapped_column(String(512), nullable=False)
    file_type: Mapped[str] = mapped_column(String(32), nullable=False, default="lci")
    phase: Mapped[str] = mapped_column(String(32), nullable=False, default="created")
    progress_pct: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    workers: Mapped[int] = mapped_column(Integer, default=4, nullable=False)
    limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    error_summary: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    stats_json: Mapped[dict | None] = mapped_column(JsonType, nullable=True)
    skipped_global: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    overwrite_existing: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class ImportJobPauseRequest(Base):
    """Marker to record a pause request."""

    __tablename__ = "import_job_pause_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DatasetCheckpoint(Base):
    """Per-dataset checkpoint for resumable import."""

    __tablename__ = "dataset_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    dataset_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True, unique=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
        # pending | running | imported | failed | skipped | skipped_global
    )
    process_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vector_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    vector_nnz: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


# ======================================================================
# Global Dataset Import State
# ======================================================================


class GlobalDatasetImport(Base):
    """Global dedup state for ecoinvent dataset imports across jobs.

    Primary key: (source_package_version, dataset_uuid)
    dataset_uuid = activity_id + reference_product_id (fallback: activity_id only)
    """

    __tablename__ = "global_dataset_imports"
    __table_args__ = (
        UniqueConstraint(
            "source_package_version",
            "dataset_uuid",
            name="uq_global_dataset_pv_uuid",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_package_version: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    dataset_uuid: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    dataset_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    process_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    activity_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reference_product_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
        # pending | imported | failed
    )
    vector_nnz: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    imported_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


# Backwards-compatible alias
Ef31Job = ImportJob
Ef31Checkpoint = DatasetCheckpoint
