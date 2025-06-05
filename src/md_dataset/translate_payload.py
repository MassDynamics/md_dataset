import json
import copy
import re

# Mapping from old property names/types to new ones
def move_required_flags(schema: dict) -> dict:
    def process_object(obj: dict):
        if not isinstance(obj, dict):
            return

        # Recurse into nested dictionaries
        for key, value in obj.items():
            if isinstance(value, dict):
                process_object(value)
            elif isinstance(value, list):
                for item in value:
                    process_object(item)

        # Move "required": [] to properties
        if 'required' in obj and 'properties' in obj:
            for required_key in obj['required']:
                if required_key in obj['properties']:
                    obj['properties'][required_key]['required'] = True
            del obj['required']

    # Start processing from the root schema
    process_object(schema)
    return schema

def resolve_refs(schema: dict) -> dict:
    definitions = schema.get("definitions", {})
    
    def _resolve(node):
        if isinstance(node, dict):
            if "$ref" in node:
                ref_path = node["$ref"]
                if ref_path.startswith("#/definitions/"):
                    def_name = ref_path.split("/")[-1]
                    resolved = copy.deepcopy(definitions[def_name])
                    # Recursively resolve the resolved value
                    return _resolve(resolved)
                else:
                    raise ValueError(f"Unsupported $ref path: {ref_path}")
            else:
                return {k: _resolve(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [_resolve(item) for item in node]
        else:
            return node

    resolved_schema = copy.deepcopy(schema)
    resolved_schema.pop("definitions", None)  # Definitions no longer needed after resolving
    return _resolve(resolved_schema)

def flatten_properties(schema: dict, key_to_flatten: str) -> dict:
    def _flatten(node):
        if isinstance(node, dict):
            props = node.get(key_to_flatten)
            other_keys = {k: v for k, v in node.items() if k != key_to_flatten}

            if isinstance(props, dict):
                flat_props = {}
                for k, v in props.items():
                    flat_props[k] = _flatten(v)
                # Only add non-conflicting keys from other_keys
                for k, v in _flatten(other_keys).items():
                    if k not in flat_props:
                        flat_props[k] = v
                return flat_props

            return {k: _flatten(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [_flatten(item) for item in node]
        else:
            return node

    return _flatten(schema)

def remove_and_promote(schema: dict, key_to_promote: str) -> dict:
    new_schema = {}
    for key, value in schema.items():
        if key == key_to_promote and isinstance(value, dict):
            # Promote all child keys of `params`
            for inner_key, inner_val in value.items():
                new_schema[inner_key] = inner_val
        else:
            new_schema[key] = value
    return new_schema

def convert_types_by_key(schema: dict, type_mapping: dict) -> dict:
    def _convert(node, key=None):
        if isinstance(node, dict):
            node = dict(node)  # shallow copy
            if key in type_mapping:
                node["type"] = type_mapping[key]
            return {k: _convert(v, k) for k, v in node.items()}
        elif isinstance(node, list):
            return [_convert(item) for item in node]
        else:
            return node

    return _convert(schema)

def convert_enums_to_options(schema: dict) -> dict:
    def format_value(v):
        # Replace spaces with underscores and lowercase
        return re.sub(r"\s+", "_", v.lower())

    def format_name(v):
        # Capitalize first letter of each word
        return v.strip().title()

    def _convert(node):
        if isinstance(node, dict):
            node = dict(node)  # shallow copy
            if "enum" in node:
                node["options"] = [
                    {"name": format_name(v), "value": format_value(v)}
                    for v in node["enum"]
                ]
                del node["enum"]  # remove original enum
            return {k: _convert(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [_convert(item) for item in node]
        else:
            return node

    return _convert(schema)



def translate_payload(payload: dict) -> dict:
    type_mapping = {
        "id": "UUID",
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
        "output_dataset_type": "String"
    }
    payload = move_required_flags(payload)
    payload = convert_enums_to_options(payload)
    payload = resolve_refs(payload)
    payload = flatten_properties(payload, "properties")
    payload = flatten_properties(payload, "items")
    payload = remove_and_promote(payload, "params")
    payload = convert_types_by_key(payload, type_mapping)
    return payload




# Example usage:
with open("src/md_dataset/payload_old.json") as f:
    payload_old = json.load(f)

payload_new = translate_payload(payload_old)

with open("src/md_dataset/payload_new_generated.json", "w") as f:
    json.dump(payload_new, f, indent=4)