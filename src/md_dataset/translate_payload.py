import copy
import re
from functools import partial


def translate_payload(payload: dict) -> dict:
    return _apply_pipeline(payload, _pipeline)

def _move_required_flags(schema: dict) -> dict:
    def process_object(obj: dict) -> None:
        if not isinstance(obj, dict):
            return

        for value in obj.values():  # PERF102, B007
            if isinstance(value, dict):
                process_object(value)
            elif isinstance(value, list):
                for item in value:
                    process_object(item)

        if "required" in obj and "properties" in obj:
            for required_key in obj["required"]:
                if required_key in obj["properties"]:
                    obj["properties"][required_key]["required"] = True
            del obj["required"]

    process_object(schema)
    return schema

def _resolve_refs(schema: dict) -> dict:
    definitions = schema.get("definitions", {})
    def _resolve(node: dict) -> dict:
        if isinstance(node, dict):
            if "$ref" in node:
                ref_path = node["$ref"]
                if ref_path.startswith("#/definitions/"):
                    def_name = ref_path.split("/")[-1]
                    resolved = copy.deepcopy(definitions[def_name])
                    return _resolve(resolved)
                msg = "Unsupported $ref path: " + ref_path  # TRY003, EM102
                raise ValueError(msg)
            return {k: _resolve(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_resolve(item) for item in node]
        return node
    resolved_schema = copy.deepcopy(schema)
    resolved_schema.pop("definitions", None)
    return _resolve(resolved_schema)

def _flatten_properties(schema: dict, key_to_flatten: str) -> dict:
    def _flatten(node: dict) -> dict:
        if isinstance(node, dict):
            props = node.get(key_to_flatten)
            other_keys = {k: v for k, v in node.items() if k != key_to_flatten}
            if isinstance(props, dict):
                flat_props = {k: _flatten(v) for k, v in props.items()}  # PERF403
                for k, v in _flatten(other_keys).items():
                    if k not in flat_props:
                        flat_props[k] = v
                return flat_props
            return {k: _flatten(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_flatten(item) for item in node]
        return node
    return _flatten(schema)

def _remove_and_promote(schema: dict, key_to_promote: str) -> dict:
    # PERF403: Use dict comprehension
    new_schema = {
        inner_key: inner_val
        for key, value in schema.items()
        if key == key_to_promote and isinstance(value, dict)
        for inner_key, inner_val in value.items()
    }
    new_schema.update({key: value for key, value in schema.items() if key != key_to_promote})
    return new_schema

def _convert_types_by_key(schema: dict, type_mapping: dict) -> dict:
    def _convert(node: dict, key: str | None = None) -> dict:  # Use Optional[str]
        if isinstance(node, dict):
            node = dict(node)
            if key is not None and key in type_mapping:
                node["type"] = type_mapping[key]
            return {k: _convert(v, k) for k, v in node.items()}
        if isinstance(node, list):
            return [_convert(item) for item in node]
        return node
    return _convert(schema)

def _convert_enums_to_options(schema: dict) -> dict:
    def format_value(v: str) -> str:
        return re.sub(r"\s+", "_", v.lower())
    def format_name(v: str) -> str:
        return v.strip().title()
    def _convert(node: dict) -> dict:
        if isinstance(node, dict):
            node = dict(node)
            if "enum" in node:
                node["options"] = [
                    {"name": format_name(v), "value": format_value(v)}
                    for v in node["enum"]
                ]
                del node["enum"]
            return {k: _convert(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_convert(item) for item in node]
        return node
    return _convert(schema)

def _move_to_parameters(schema: dict, keys_to_move: list) -> dict:
    def _move(node: dict) -> dict:
        if isinstance(node, dict):
            node = dict(node)
            for key_to_move in keys_to_move:
                if key_to_move in node:
                    value = node.pop(key_to_move)
                    if "parameters" not in node:
                        node["parameters"] = {}
                    node["parameters"][key_to_move] = value
            return {
                k: _move(v) if k != "parameters" else v
                for k, v in node.items()
            }
        if isinstance(node, list):
            return [_move(item) for item in node]
        return node
    return _move(schema)

def _apply_pipeline(payload: dict, transforms: list) -> dict:
    for transform in transforms:
        payload = transform(payload)
    return payload

def _rename_keys(schema: dict, key_mapping: dict) -> dict:
    def _rename(node: dict) -> dict:
        if isinstance(node, dict):
            new_node = {}
            for k, v in node.items():
                new_key = key_mapping.get(k, k)
                new_node[new_key] = _rename(v)
            return new_node
        if isinstance(node, list):
            return [_rename(item) for item in node]
        return node

    return _rename(schema)

def _expand_one_of_blocks(schema: dict) -> dict:
    def _expand(node: dict) -> dict:
        if isinstance(node, dict):
            new_node = {}
            for k, v in node.items():
                if isinstance(v, dict) and "oneOf" in v and "discriminator" in v:
                    discriminator = v["discriminator"]
                    mapping = discriminator.get("mapping", {})
                    one_of_list = v["oneOf"]

                    for variant in one_of_list:
                        title = variant.get("title")
                        variant_key = next(
                            (k_ for k_, v_ in mapping.items() if title in v_),
                            title,
                        )
                        new_node.setdefault(k, {})[variant_key] = _expand(variant)

                    for key in v:
                        if key not in {"oneOf", "discriminator"}:
                            new_node[k][key] = _expand(v[key])
                else:
                    new_node[k] = _expand(v)
            return new_node
        if isinstance(node, list):
            return [_expand(item) for item in node]
        return node

    return _expand(schema)

def _expand_any_of_blocks(schema: dict) -> dict:
    def _clean(node: dict) -> dict:
        if isinstance(node, dict):
            # PERF403: Use dict comprehension
            any_of_items = {
                nk: nv
                for key, value in node.items()
                if key == "anyOf" and isinstance(value, list)
                for non_null in [next((item for item in value if item.get("type") != "null"), None)]
                if non_null
                for nk, nv in _clean(non_null).items()
            }
            other_items = {
                key: _clean(value)
                for key, value in node.items()
                if not (key == "anyOf" and isinstance(value, list))
            }
            return {**any_of_items, **other_items}
        if isinstance(node, list):
            return [_clean(item) for item in node]
        return node

    return _clean(schema)




_type_mapping = {
    "id": "UUID",
    "user_id": "UUID",
    "experiment_id": "UUID",
    "experiment_ids": "Array",
    "name": "String",
    "job_run_params": "__REPLACE_ME__",
    "type": "String",
    "tables": "Array",
    "dataset_name": "String",
    "sample_name": "String",
    "experiment_design": "Experiment",
    "condition_column": "ConditionComparison",
    "condition_comparisons": "ConditionComparisonConditions",
    "condition_comparison_pairs": "Array",
    "control_variables": "Array",
    "column": "String",
    "limma_trend": "Boolean",
    "robust_empirical_bayes": "Boolean",
    "fit_separate_models": "Boolean",
    "filter_values_criteria": "__REPLACE_ME__",
    "method": "String",
    "filter_threshold_percentage": "Number",
    "filter_threshold_count": "Number",
    "filter_valid_values_logic": "String",
    "output_dataset_type": "String",
}

_key_mapping = {
    "maxItems": "max",
    "minItems": "min",
    "maximum": "max",
    "minimum": "min",
}

_pipeline = [
    _move_required_flags,
    _convert_enums_to_options,
    _resolve_refs,
    partial(_rename_keys, key_mapping=_key_mapping),
    partial(_move_to_parameters, keys_to_move=["options", "min", "max"]),
    partial(_flatten_properties, key_to_flatten="properties"),
    partial(_flatten_properties, key_to_flatten="items"),
    partial(_remove_and_promote, key_to_promote="params"),
    partial(_convert_types_by_key, type_mapping=_type_mapping),
    _expand_one_of_blocks,
    _expand_any_of_blocks,
]

