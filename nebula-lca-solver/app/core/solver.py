from app.core.schema import ComputeRequest, ComputeResponse


def solve_compute(req: ComputeRequest) -> ComputeResponse:
    # TODO: replace with matrix-based solver
    # v0.1 placeholder: sum elementary flows directly from exchanges
    lci = {}
    for ex in req.exchanges:
        delta = ex.amount if ex.direction == "output" else -ex.amount
        lci[ex.flow_uuid] = lci.get(ex.flow_uuid, 0.0) + delta

    # single-indicator LCIA using req.characterization
    score = 0.0
    for flow_uuid, amt in lci.items():
        cf = req.characterization.get(flow_uuid, 0.0)
        score += amt * cf

    return ComputeResponse(
        lci=lci,
        lcia={"indicator_1": score},
        debug={"note": "placeholder implementation; replace with matrix solver"},
    )
