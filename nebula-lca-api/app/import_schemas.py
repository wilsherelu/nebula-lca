from dataclasses import dataclass


@dataclass(frozen=True)
class FieldSpec:
    target: str
    aliases: tuple[str, ...]
    required: bool = True


FLOW_IMPORT_SPECS = (
    FieldSpec("flow_uuid", ("flow_uuid", "uuid", "flow id", "flow_id", "流uuid", "流id")),
    FieldSpec("flow_name", ("flow_name", "name", "flow", "flow name", "流名称", "名称", "flow_name_zh", "流名称中文")),
    FieldSpec("flow_name_en", ("flow_name_en", "name_en", "flow en", "流名称英文"), required=False),
    FieldSpec("flow_type", ("flow_type", "type", "flow type", "流类型", "类别"), required=False),
    FieldSpec(
        "default_unit",
        ("default_unit", "unit", "default_unit_uuid", "默认单位", "单位"),
    ),
    FieldSpec(
        "unit_group",
        ("unit_group", "unit_group_uuid", "unit group", "单位组", "单位组uuid"),
    ),
    FieldSpec("compartment", ("compartment", "环境介质", "排放介质"), required=False),
    FieldSpec("updated_at", ("updated_at", "更新时间", "last_updated"), required=False),
)
