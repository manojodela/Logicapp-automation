"""
Schema Validator Module
Validates generated Logic App workflow JSON against the Azure Standard Logic App schema.
Falls back to structural validation when the full JSON schema file is not present.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List

from utils.logger import get_logger

logger = get_logger("validator")

# Minimum required keys for a valid Standard Logic App workflow content object
REQUIRED_TOP_LEVEL_KEYS = {"definition", "parameters"}
REQUIRED_DEFINITION_KEYS = {"$schema", "contentVersion", "triggers", "actions"}

EXPECTED_SCHEMA_URL = (
    "https://schema.management.azure.com/providers/Microsoft.Logic/"
    "schemas/2016-06-01/workflowdefinition.json#"
)
EXPECTED_CONTENT_VERSION = "1.0.0.0"


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class SchemaValidator:
    def __init__(self, config):
        self.config = config
        self._jsonschema_validator = None
        self._load_jsonschema_validator()

    def _load_jsonschema_validator(self):
        """Attempt to load a full JSON schema validator. Gracefully degrades if unavailable."""
        schema_path: Path = self.config.schema_path
        if not schema_path.exists():
            logger.warning(
                "ARM schema file not found — falling back to structural validation",
                extra={"schema_path": str(schema_path)},
            )
            return

        try:
            import jsonschema  # type: ignore

            with open(schema_path, "r", encoding="utf-8") as f:
                schema = json.load(f)

            self._jsonschema_validator = jsonschema.Draft7Validator(schema)
            logger.info("Full JSON schema validator loaded", extra={"schema_path": str(schema_path)})
        except ImportError:
            logger.warning("jsonschema library not installed — falling back to structural validation")
        except Exception as e:
            logger.warning(
                "Failed to load schema file — falling back to structural validation",
                extra={"error": str(e)},
            )

    def validate(self, workflow_json: Any) -> ValidationResult:
        """
        Validate a workflow definition dict.

        Runs full JSON schema validation if the schema + jsonschema library are available.
        Always runs structural validation checks regardless.
        """
        errors: List[str] = []
        warnings: List[str] = []

        # ── Type check ─────────────────────────────────────────
        if not isinstance(workflow_json, dict):
            return ValidationResult(
                is_valid=False,
                errors=[f"Workflow must be a JSON object; got {type(workflow_json).__name__}"],
            )

        # ── Structural validation ──────────────────────────────
        errors.extend(self._structural_validation(workflow_json))

        # ── Full JSON schema validation ────────────────────────
        if self._jsonschema_validator is not None and not errors:
            schema_errors = self._run_jsonschema_validation(workflow_json)
            errors.extend(schema_errors)

        is_valid = len(errors) == 0
        logger.info(
            "Schema validation complete",
            extra={"is_valid": is_valid, "error_count": len(errors), "warning_count": len(warnings)},
        )
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def _structural_validation(self, workflow_json: dict) -> List[str]:
        """Core structural checks that do not require the schema file."""
        errors = []

        # Top-level keys
        missing_top = REQUIRED_TOP_LEVEL_KEYS - set(workflow_json.keys())
        if missing_top:
            errors.append(f"Missing top-level keys: {sorted(missing_top)}")
            return errors  # Cannot validate further without 'definition'

        definition = workflow_json.get("definition", {})

        if not isinstance(definition, dict):
            errors.append("'definition' must be a JSON object")
            return errors

        # Required definition keys
        missing_def = REQUIRED_DEFINITION_KEYS - set(definition.keys())
        if missing_def:
            errors.append(f"Missing keys inside 'definition': {sorted(missing_def)}")

        # $schema value
        schema_url = definition.get("$schema", "")
        if schema_url != EXPECTED_SCHEMA_URL:
            errors.append(
                f"'definition.$schema' must be '{EXPECTED_SCHEMA_URL}'; got '{schema_url}'"
            )

        # contentVersion value
        content_version = definition.get("contentVersion", "")
        if content_version != EXPECTED_CONTENT_VERSION:
            errors.append(
                f"'definition.contentVersion' must be '{EXPECTED_CONTENT_VERSION}'; got '{content_version}'"
            )

        # triggers and actions must be dicts
        for key in ("triggers", "actions"):
            val = definition.get(key)
            if val is not None and not isinstance(val, dict):
                errors.append(f"'definition.{key}' must be a JSON object; got {type(val).__name__}")

        # parameters must be a dict
        params = workflow_json.get("parameters", {})
        if not isinstance(params, dict):
            errors.append(f"'parameters' must be a JSON object; got {type(params).__name__}")

        # Each action must have a 'type' field
        actions = definition.get("actions", {})
        if isinstance(actions, dict):
            for action_name, action_body in actions.items():
                if isinstance(action_body, dict) and "type" not in action_body:
                    errors.append(f"Action '{action_name}' is missing required field 'type'")

        # Each trigger must have a 'type' field
        triggers = definition.get("triggers", {})
        if isinstance(triggers, dict):
            for trigger_name, trigger_body in triggers.items():
                if isinstance(trigger_body, dict) and "type" not in trigger_body:
                    errors.append(f"Trigger '{trigger_name}' is missing required field 'type'")

        return errors

    def _run_jsonschema_validation(self, workflow_json: dict) -> List[str]:
        """Run full JSON schema validation and return human-readable error strings."""
        errors = []
        try:
            for error in self._jsonschema_validator.iter_errors(workflow_json):
                path = " -> ".join(str(p) for p in error.absolute_path) or "(root)"
                errors.append(f"[{path}] {error.message}")
        except Exception as e:
            errors.append(f"Schema validation engine error: {str(e)}")
        return errors
