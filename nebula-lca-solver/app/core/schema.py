from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Process(BaseModel):
    uuid: str
    name: Optional[str] = None


class Flow(BaseModel):
    uuid: str
    name: Optional[str] = None
    flow_type: str = Field(..., description="product or elementary")


class Exchange(BaseModel):
    process_uuid: str
    flow_uuid: str
    direction: str = Field(..., description="input or output")
    amount: float
    unit: Optional[str] = None
    is_reference_product: bool = False
    provider_process_uuid: Optional[str] = None


class Demand(BaseModel):
    flow_uuid: str
    amount: float
    unit: Optional[str] = None


class ComputeRequest(BaseModel):
    processes: List[Process] = []
    flows: List[Flow] = []
    exchanges: List[Exchange]
    demand: Demand
    characterization: Dict[str, float] = Field(
        default_factory=dict,
        description="elementary_flow_uuid -> CF (single-indicator in v0.1)",
    )


class ComputeResponse(BaseModel):
    lci: Dict[str, float]
    lcia: Dict[str, float]
    debug: Dict[str, str] = {}


class LciaPayload(BaseModel):
    snapshot: Dict


class LciaResponse(BaseModel):
    summary: Dict
    missing_ef31_flow_uuids: List[str] = []
    missing_ef31_flows: List[Dict] = []
    indicator_index: List[Dict]
    process_index: List[str]
    values: List[List[float]]
    mmr_path: Optional[str] = None
    issues: List[str] = []


class PtsCompilePayload(BaseModel):
    payload: Dict


class PtsCompileResponse(BaseModel):
    matrix_size: int
    invertible: bool
    warnings: List[str] = []
    virtual_processes: List[Dict] = []
