import copy
import re
from functools import partial
from collections import OrderedDict


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
                    # Split the path to handle nested property references
                    path_parts = ref_path.split("/")
                    def_name = path_parts[2]  # Get the definition name
                    
                    if def_name not in definitions:
                        msg = f"Definition not found: {def_name}"  # TRY003, EM102
                        raise ValueError(msg)
                    
                    # Start with the base definition
                    resolved = copy.deepcopy(definitions[def_name])
                    
                    # Navigate through the nested path if it exists
                    if len(path_parts) > 3:
                        current = resolved
                        for part in path_parts[3:]:
                            if isinstance(current, dict) and part in current:
                                current = current[part]
                            else:
                                msg = f"Invalid path in $ref: {ref_path}"  # TRY003, EM102
                                raise ValueError(msg)
                        resolved = current
                    
                    # Preserve original values for duplicate keys
                    original_keys = {k: v for k, v in node.items() if k != "$ref"}
                    resolved = _resolve(resolved)
                    # Merge resolved with original, keeping original values for duplicates
                    merged = {**resolved, **original_keys}
                    return merged
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

def _resolve_one_of(schema: dict) -> dict:
    """Resolve oneOf blocks by extracting referenced properties to top level, placing them after the source property."""
    definitions = schema.get("definitions", {})
    
    def _resolve(node: dict, parent_node: dict = None) -> dict:
        if isinstance(node, dict):
            new_node = OrderedDict()
            items = list(node.items())
            i = 0
            while i < len(items):
                key, value = items[i]
                if key == "oneOf" and isinstance(value, list):
                    i += 1
                    continue
                if isinstance(value, dict):
                    resolved_value = _resolve(value, new_node)
                    new_node[key] = resolved_value
                    # If this value has a oneOf, insert extracted properties after this property
                    if "oneOf" in value and isinstance(value["oneOf"], list):
                        extracted_properties = OrderedDict()
                        for one_of_item in value["oneOf"]:
                            if isinstance(one_of_item, dict) and "$ref" in one_of_item:
                                ref_path = one_of_item["$ref"]
                                if ref_path.startswith("#/definitions/"):
                                    def_name = ref_path.split("/")[-1]
                                    if def_name in definitions:
                                        def_obj = definitions[def_name]
                                        if "properties" in def_obj:
                                            for prop_name, prop_value in def_obj["properties"].items():
                                                if prop_name != "method":
                                                    full_ref_path = f"{ref_path}/properties/{prop_name}"
                                                    extracted_properties[prop_name] = {"$ref": full_ref_path}
                        for prop_name, prop_value in extracted_properties.items():
                            new_node[prop_name] = prop_value
                elif isinstance(value, list):
                    new_node[key] = [_resolve(item) for item in value]
                else:
                    new_node[key] = value
                i += 1
            return new_node
        elif isinstance(node, list):
            return [_resolve(item) for item in node]
        else:
            return node
    resolved_schema = copy.deepcopy(schema)
    return _resolve(resolved_schema)


_key_mapping = {
    "maxItems": "max",
    "minItems": "min",
    "maximum": "max",
    "minimum": "min",
}

_pipeline = [
    _move_required_flags,
    _convert_enums_to_options,
    _resolve_one_of,
    _resolve_refs,
    partial(_rename_keys, key_mapping=_key_mapping),
    partial(_move_to_parameters, keys_to_move=["options", "min", "max"]),
    partial(_flatten_properties, key_to_flatten="properties"),
    partial(_flatten_properties, key_to_flatten="items"),
    partial(_remove_and_promote, key_to_promote="params"),
]

# Example usage:
import json

with open("src/md_dataset/payload_full.json") as f:
    payload_old = json.load(f)

payload_new = []
for payload in payload_old:
    print(f"Translating payload: {payload['name']}")
    pl = {
        "name": payload["name"],
        "type": payload["run_type"],
        "properties":  translate_payload(payload["required_params"]),

    }
    payload_new.append(pl)

with open("src/md_dataset/payload_new_generated.json", "w") as f:
    json.dump(payload_new, f, indent=4)