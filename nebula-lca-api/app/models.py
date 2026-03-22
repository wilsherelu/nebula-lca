from datetime import datetime
import uuid
from sqlalchemy import String, Integer, DateTime, ForeignKey, Text, Float, Boolean, UniqueConstraint
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


class UnitGroup(Base):
    __tablename__ = "unit_groups"

    name: Mapped[str] = mapped_column(String(128), primary_key=True)
    reference_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)


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
