"""
Configuration Manager
Loads and validates all required configuration from environment variables.
Fails fast with descriptive errors if required values are missing.
"""
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    # Azure Identity
    subscription_id: str = ""
    resource_group: str = ""
    logic_app_name: str = ""
    azure_location: str = ""
    arm_api_version: str = "2025-05-01"

    # LLM (Groq)
    llm_api_key: str = ""
    # llm_model: str = "llama-3.3-70b-versatile"
    llm_model: str = "openai/gpt-oss-120b"
    llm_api_url: str = "https://api.groq.com/openai/v1/chat/completions"
    llm_max_tokens: int = 4096
    llm_timeout_seconds: int = 60

    # Pipeline
    max_retries: int = 5
    retry_backoff_base_seconds: float = 2.0
    retry_backoff_max_seconds: float = 30.0

    # File paths
    output_dir: Path = Path("workflows/generated")
    failed_dir: Path = Path("workflows/failed")
    schema_path: Path = Path("config/logic_app_schema_2025_05_01.json")
    log_dir: Path = Path("logs")
    log_level: str = "INFO"

    # Azure CLI
    az_cli_path: str = "az"
    az_deployment_timeout_seconds: int = 120

    @classmethod
    def load(cls) -> "Config":
        cfg = cls(
            subscription_id=os.environ.get("AZURE_SUBSCRIPTION_ID"),
            resource_group=os.environ.get("AZURE_RESOURCE_GROUP"),
            logic_app_name=os.environ.get("AZURE_LOGIC_APP_NAME"),
            azure_location=os.environ.get("AZURE_LOCATION"),
            arm_api_version=os.environ.get("ARM_API_VERSION"),

            llm_api_key=os.environ.get("GROQ_API_KEY", os.environ.get("LLM_API_KEY")),
            llm_model=os.environ.get("LLM_MODEL"),
            llm_api_url=os.environ.get("LLM_API_URL"),
            llm_max_tokens=int(os.environ.get("LLM_MAX_TOKENS")),
            llm_timeout_seconds=int(os.environ.get("LLM_TIMEOUT_SECONDS")),

            max_retries=int(os.environ.get("MAX_RETRIES", "")),
            retry_backoff_base_seconds=float(os.environ.get("RETRY_BACKOFF_BASE")),
            retry_backoff_max_seconds=float(os.environ.get("RETRY_BACKOFF_MAX")),

            output_dir=Path(os.environ.get("OUTPUT_DIR", "workflows/generated")),
            failed_dir=Path(os.environ.get("FAILED_DIR", "workflows/failed")),
            schema_path=Path(os.environ.get("SCHEMA_PATH", "config/logic_app_schema_2025_05_01.json")),
            log_dir=Path(os.environ.get("LOG_DIR", "logs")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),

            az_cli_path=os.environ.get("AZ_CLI_PATH", "az"),
            az_deployment_timeout_seconds=int(os.environ.get("AZ_DEPLOY_TIMEOUT", "120")),
        )

        cfg._validate()
        cfg._ensure_directories()
        return cfg

    def _validate(self):
        errors = []
        required = {
            "GROQ_API_KEY": self.llm_api_key,
            "AZURE_SUBSCRIPTION_ID": self.subscription_id,
            "AZURE_RESOURCE_GROUP": self.resource_group,
            "AZURE_LOGIC_APP_NAME": self.logic_app_name,
        }
        for name, value in required.items():
            if not value or not value.strip():
                errors.append(f"Missing required configuration: {name}")

        if self.max_retries < 1:
            errors.append("MAX_RETRIES must be >= 1")

        if errors:
            raise ValueError(
                "Configuration validation failed:\n" + "\n".join(f"  • {e}" for e in errors)
            )

    def _ensure_directories(self):
        for path in [self.output_dir, self.failed_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
