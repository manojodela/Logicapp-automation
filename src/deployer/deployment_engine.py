"""
Deployment Engine Module
Deploys Azure Standard Logic App workflows via az CLI.

SAFETY FIRST:
- Pre-deployment check lists ALL existing workflows on the Logic App
- Only deploys if the workflow is NEW or explicitly allowed to update
- Never deletes or replaces existing workflows unless --allow-update flag is set
- Prints a full pre-flight summary before any changes are made

Windows fix: uses 'az.cmd' on Windows since az is a batch script, not a binary.
"""
import asyncio
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List

from utils.logger import get_logger

logger = get_logger("deployer")


def _resolve_az_path() -> str:
    """
    Resolve the correct az CLI executable for the current OS.

    On Windows, 'az' is a batch script (az.cmd), not a binary.
    subprocess.run('az') fails on Windows unless shell=True or we use 'az.cmd'.
    This function returns the correct invocation string.
    """
    if platform.system().lower() == "windows":
        # Try az.cmd first (standard Azure CLI install on Windows)
        import shutil
        if shutil.which("az.cmd"):
            return "az.cmd"
        if shutil.which("az"):
            return "az"
        # Fallback to common install paths
        common_paths = [
            r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
            r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
        ]
        for p in common_paths:
            if Path(p).exists():
                return p
        raise RuntimeError(
            "Azure CLI not found on Windows.\n"
            "Install from: https://aka.ms/installazurecliwindows\n"
            "Then restart your terminal and try again."
        )
    return "az"


def _run_az(args: list, timeout: int = 60) -> subprocess.CompletedProcess:
    """
    Run an az CLI command cross-platform.

    On Windows uses shell=True so batch scripts (az.cmd) work correctly.
    On Linux/Mac runs directly.
    """
    az_cmd = _resolve_az_path()
    full_cmd = [az_cmd] + args

    if platform.system().lower() == "windows":
        # On Windows, join as a string and use shell=True
        # This ensures az.cmd / batch scripts execute correctly
        cmd_str = " ".join(f'"{c}"' if " " in str(c) else str(c) for c in full_cmd)
        return subprocess.run(
            cmd_str,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    else:
        return subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


@dataclass
class DeploymentResult:
    success: bool
    error_message: str = ""
    raw_stdout: str = ""
    raw_stderr: str = ""
    correlation_id: str = ""
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class PreflightReport:
    workflow_name: str
    action: str          # "CREATE", "UPDATE", "SKIP"
    reason: str
    existing_definition_hash: str = ""
    new_definition_hash: str = ""
    existing_workflows_on_app: List[str] = field(default_factory=list)


class DeploymentEngine:
    def __init__(self, config):
        self.config = config
        self._az = _resolve_az_path()
        logger.info(f"az CLI resolved", extra={"az_path": self._az, "os": platform.system()})
        self._verify_az_cli()

    # ── az CLI helpers ────────────────────────────────────────────────────────

    def _verify_az_cli(self):
        """Verify az CLI is available and the session is authenticated."""
        try:
            result = _run_az(["account", "show", "--output", "json"], timeout=30)
            if result.returncode != 0:
                raise RuntimeError(
                    f"az CLI is not authenticated.\n"
                    f"Run 'az login' first.\n"
                    f"stderr: {result.stderr[:300]}"
                )
            account = json.loads(result.stdout)
        except FileNotFoundError as e:
            raise RuntimeError(
                f"Azure CLI not found. Install from https://aka.ms/installazurecliwindows\n"
                f"Detail: {e}"
            )

    # def _build_arm_url(self, workflow_name: str) -> str:
    #     sub = self.config.subscription_id
    #     rg  = self.config.resource_group
    #     app = self.config.logic_app_name
    #     api = "2025-05-01" 
    #     return (
    #         f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
    #         f"/providers/Microsoft.Web/sites/{app}/workflows/{workflow_name}"
    #         f"?api-version={api}"
    #     )

    # def _build_list_url(self) -> str:
    #     sub = self.config.subscription_id
    #     rg  = self.config.resource_group
    #     app = self.config.logic_app_name
    #     api = "2025-05-01"
    #     return (
    #         f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
    #         f"/providers/Microsoft.Web/sites/{app}/workflows"
    #         f"?api-version={api}"
    #     )
    
    def _build_arm_url(self, workflow_name: str) -> str:
        sub = self.config.subscription_id
        rg  = self.config.resource_group
        app = self.config.logic_app_name
        api = "2025-05-01" 
        return (
            f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Web/sites/{app}/extensions/vfs/site/wwwroot/{workflow_name}/workflow.json"
            f"?api-version={api}"
        )

    def _build_list_url(self) -> str:
        sub = self.config.subscription_id
        rg  = self.config.resource_group
        app = self.config.logic_app_name
        api = "2025-05-01"
        return (
            f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Web/sites/{app}/workflows"
            f"?api-version={api}"
        )

    def _az_get(self, url: str, timeout: int = 30) -> Optional[dict]:
        """Run az rest GET and return parsed JSON, or None on failure."""
        result = _run_az(["rest", "--method", "GET", "--url", url, "--output", "json"], timeout=timeout)
        if result.returncode != 0:
            logger.debug("az GET failed", extra={"url": url[:100], "stderr": result.stderr[:200]})
            return None
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return None

    def _definition_hash(self, workflow_json: dict) -> str:
        definition = workflow_json.get("definition", workflow_json)
        canonical  = json.dumps(definition, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def _extract_error_message(self, stdout: str, stderr: str, returncode: int) -> str:
        """
        Extract detailed error message from az CLI output.
        Azure REST API errors are typically in JSON format with error.code, error.message, etc.
        """
        # First, try to find JSON error responses (most common case)
        for candidate in [stdout, stderr]:
            if not candidate.strip():
                continue
            
            # Look for JSON error objects
            try:
                data = json.loads(candidate)
                if "error" in data:
                    err = data["error"]
                    code = err.get("code", "")
                    message = err.get("message", "")
                    details = err.get("details", [])
                    target = err.get("target", "")
                    
                    full = f"[{code}] {message}"
                    if target:
                        full += f" (Target: {target})"
                    if details:
                        detail_msgs = []
                        for d in details:
                            if isinstance(d, dict) and "message" in d:
                                detail_msgs.append(d["message"])
                        if detail_msgs:
                            full += f" | Details: {'; '.join(detail_msgs)}"
                    return full
            except (json.JSONDecodeError, TypeError):
                pass
            
            # Look for HTTP status codes and error patterns
            lines = candidate.split('\n')
            for line in lines:
                line = line.strip()
                # Look for HTTP status codes
                if 'HTTP/1.1' in line or 'HTTP/2' in line:
                    status_match = re.search(r'HTTP/[0-9.]+\s+(\d{3})\s+(.+)', line)
                    if status_match:
                        status_code = status_match.group(1)
                        status_text = status_match.group(2)
                        return f"HTTP {status_code} {status_text}"
                
                # Look for error patterns in plain text
                if line.startswith('ERROR:') or 'error' in line.lower() and ('failed' in line.lower() or 'invalid' in line.lower()):
                    return line[:1000]
        
        # Fallback: return combined output
        combined = (stderr or stdout or "").strip()
        if combined:
            # Try to extract the most relevant error line
            lines = combined.split('\n')
            for line in lines:
                if 'ERROR' in line.upper() or 'FAILED' in line.upper() or 'Bad Request' in line or 'Unauthorized' in line:
                    return line.strip()[:1000]
            return combined[:1000]
        
        return f"Deployment failed with exit code {returncode}"

    def _get_detailed_error_info(self, stdout: str, stderr: str, returncode: int) -> dict:
        """
        Extract comprehensive error information for debugging.
        Returns a dict with various error details found in the output.
        """
        error_info = {
            "returncode": returncode,
            "stdout_length": len(stdout),
            "stderr_length": len(stderr),
            "extracted_error": self._extract_error_message(stdout, stderr, returncode),
            "stdout_preview": stdout[:500] if stdout else "",
            "stderr_preview": stderr[:500] if stderr else "",
        }
        
        # Try to parse JSON responses
        for source, content in [("stdout", stdout), ("stderr", stderr)]:
            if content.strip():
                try:
                    data = json.loads(content)
                    error_info[f"{source}_json"] = data
                    if "error" in data:
                        error_info["azure_error"] = data["error"]
                except json.JSONDecodeError:
                    pass
        
        # Look for HTTP status lines
        combined = stdout + stderr
        http_lines = [line for line in combined.split('\n') if 'HTTP/' in line]
        if http_lines:
            error_info["http_status_lines"] = http_lines
        
        return error_info
    
    def _parse_correlation_id(self, response_data):
        # Example implementation: extract the ID from a JSON dict or headers
        if isinstance(response_data, dict):
            return response_data.get("correlationId", "No-Correlation-ID")
        return "No-Correlation-ID"


    def _exponential_backoff(self, attempt: int):
        import random
        base     = self.config.retry_backoff_base_seconds
        max_wait = self.config.retry_backoff_max_seconds
        wait     = min(base * (2 ** (attempt - 1)), max_wait)
        jitter   = random.uniform(0, wait * 0.1)
        total    = wait + jitter
        logger.info(f"Back-off: waiting {total:.1f}s before next attempt")
        time.sleep(total)

    # ── Pre-flight safety check ───────────────────────────────────────────────

    async def preflight_check(
        self,
        workflow_json: dict,
        workflow_name: str,
        allow_update: bool = False,
    ) -> PreflightReport:
        """
        Inspect what already exists on Azure BEFORE making any changes.

        Decision table:
          Workflow NOT found   → CREATE  (always safe)
          Workflow found + allow_update=False → SKIP  (protect existing)
          Workflow found + allow_update=True  + no change → SKIP  (no-op)
          Workflow found + allow_update=True  + changed   → UPDATE
        """
        loop      = asyncio.get_event_loop()
        list_url  = self._build_list_url()
        new_hash  = self._definition_hash(workflow_json)

        logger.info("Pre-flight: listing all existing workflows on Logic App",
                    extra={"logic_app": self.config.logic_app_name})

        list_response      = await loop.run_in_executor(None, lambda: self._az_get(list_url))
        existing_workflows: List[str] = []

        if list_response and "value" in list_response:
            existing_workflows = [
                w.get("name", "").split("/")[-1]
                for w in list_response["value"]
            ]

        logger.info(f"Pre-flight: found {len(existing_workflows)} existing workflow(s)",
                    extra={"workflows": existing_workflows})

        # ── Not found → CREATE ────────────────────────────────────
        if workflow_name not in existing_workflows:
            report = PreflightReport(
                workflow_name=workflow_name,
                action="CREATE",
                reason="Workflow does not exist on the Logic App — safe to create.",
                new_definition_hash=new_hash,
                existing_workflows_on_app=existing_workflows,
            )
            self._print_preflight_summary(report)
            return report

        # ── Found, no update flag → SKIP ─────────────────────────
        if not allow_update:
            report = PreflightReport(
                workflow_name=workflow_name,
                action="SKIP",
                reason=(
                    f"Workflow '{workflow_name}' already exists and --allow-update is NOT set. "
                    f"Skipping to protect existing resource. "
                    f"Re-run with --allow-update to overwrite."
                ),
                new_definition_hash=new_hash,
                existing_workflows_on_app=existing_workflows,
            )
            self._print_preflight_summary(report)
            return report

        # ── Found + allow_update → check if definition changed ───
        get_url           = self._build_arm_url(workflow_name)
        existing_resource = await loop.run_in_executor(None, lambda: self._az_get(get_url))
        existing_hash     = ""

        if existing_resource:
            existing_definition = existing_resource.get("properties", {})
            existing_hash       = self._definition_hash(existing_definition)

        if existing_hash == new_hash:
            report = PreflightReport(
                workflow_name=workflow_name,
                action="SKIP",
                reason="Workflow definition is identical to what is already deployed — no changes needed.",
                existing_definition_hash=existing_hash,
                new_definition_hash=new_hash,
                existing_workflows_on_app=existing_workflows,
            )
            self._print_preflight_summary(report)
            return report

        report = PreflightReport(
            workflow_name=workflow_name,
            action="UPDATE",
            reason=(
                f"Workflow exists and --allow-update is set. "
                f"Definition changed (hash: {existing_hash} → {new_hash})."
            ),
            existing_definition_hash=existing_hash,
            new_definition_hash=new_hash,
            existing_workflows_on_app=existing_workflows,
        )
        self._print_preflight_summary(report)
        return report

    def _print_preflight_summary(self, report: PreflightReport):
        divider = "=" * 65
        print(f"\n{divider}")
        print(f"  PRE-FLIGHT DEPLOYMENT SUMMARY")
        print(divider)
        print(f"  Logic App   : {self.config.logic_app_name}")
        print(f"  Resource Grp: {self.config.resource_group}")
        print(f"  Workflow    : {report.workflow_name}")
        print(f"  Action      : {report.action}")
        print(f"  Reason      : {report.reason}")
        print(f"\n  Existing workflows on Logic App ({len(report.existing_workflows_on_app)}):")
        if report.existing_workflows_on_app:
            for wf in report.existing_workflows_on_app:
                tag = "  ← will be updated" if wf == report.workflow_name and report.action == "UPDATE" else ""
                print(f"    • {wf}{tag}")
        else:
            print("    (none found — or Logic App not yet reachable)")
        print(divider)

        icons = {"SKIP": "⏭  SKIPPED", "CREATE": "✅ SAFE CREATE", "UPDATE": "⚠️  UPDATE"}
        msgs  = {
            "SKIP":   "No changes will be made to Azure.",
            "CREATE": "A brand-new workflow will be created.",
            "UPDATE": "Existing workflow WILL be overwritten.",
        }
        print(f"\n  {icons[report.action]} — {msgs[report.action]}\n")

    # ── Deployment ────────────────────────────────────────────────────────────

    async def deploy(
        self,
        workflow_json: dict,
        workflow_name: str,
        workflow_file_path: Path,
        allow_update: bool = False,
    ) -> DeploymentResult:
        """
        Safe deployment with pre-flight check.
        Skips if workflow exists and --allow-update not set.
        """
        report = await self.preflight_check(workflow_json, workflow_name, allow_update)

        if report.action == "SKIP":
            return DeploymentResult(
                success=True,
                skipped=True,
                skip_reason=report.reason,
            )

        arm_payload  = {
            "properties": {
                "definition": workflow_json.get("definition", {}),
                "parameters": workflow_json.get("parameters", {}),
            }
        }
        payload_path = workflow_file_path.parent / f"{workflow_file_path.stem}_arm_payload.json"

        try:
            with open(payload_path, "w", encoding="utf-8") as f:
                json.dump(arm_payload, f, indent=2)

            url = self._build_arm_url(workflow_name)
            logger.info("Executing deployment", extra={
                "action": report.action,
                "workflow_name": workflow_name,
            })

            loop = asyncio.get_event_loop()
            proc = await loop.run_in_executor(
                None,
                lambda: _run_az(
                    ["rest", "--method", "PUT", "--url", url,
                     "--body", f"@{payload_path}", "--output", "json"],
                    timeout=self.config.az_deployment_timeout_seconds,
                )
            )

            correlation_id = self._parse_correlation_id(proc.stderr)

            if proc.returncode == 0:
                verify = await self._verify_deployment(workflow_name)
                if verify.success:
                    print(f"\n  ✅ Deployment SUCCESS — '{workflow_name}' is live on Azure.\n")
                    return DeploymentResult(
                        success=True,
                        raw_stdout=proc.stdout,
                        raw_stderr=proc.stderr,
                        correlation_id=correlation_id,
                    )
                return verify

            error_msg = self._extract_error_message(proc.stdout, proc.stderr, proc.returncode)
            
            # Get detailed error information for better debugging
            detailed_error = self._get_detailed_error_info(proc.stdout, proc.stderr, proc.returncode)
            
            logger.warning("Deployment failed", extra={
                "error": error_msg[:300],
                "stdout_length": len(proc.stdout),
                "stderr_length": len(proc.stderr),
                "returncode": proc.returncode,
                "correlation_id": correlation_id,
                "azure_error": detailed_error.get("azure_error"),
                "http_status_lines": detailed_error.get("http_status_lines"),
            })
            
            # Log full output for debugging (first 1000 chars each)
            if proc.stdout:
                logger.debug("Deployment stdout", extra={"stdout": proc.stdout[:1000]})
            if proc.stderr:
                logger.debug("Deployment stderr", extra={"stderr": proc.stderr[:1000]})
            
            return DeploymentResult(
                success=False,
                error_message=error_msg,
                raw_stdout=proc.stdout,
                raw_stderr=proc.stderr,
                correlation_id=correlation_id,
            )

        except subprocess.TimeoutExpired:
            return DeploymentResult(
                success=False,
                error_message=f"Deployment timed out after {self.config.az_deployment_timeout_seconds}s",
            )
        except Exception as e:
            return DeploymentResult(
                success=False,
                error_message=f"Unexpected deployment error: {str(e)}",
            )
        finally:
            if payload_path.exists():
                try:
                    payload_path.unlink()
                except OSError:
                    pass

    async def _verify_deployment(self, workflow_name: str) -> DeploymentResult:
        """GET the resource after PUT to confirm it is in Enabled state."""
        loop     = asyncio.get_event_loop()
        url      = self._build_arm_url(workflow_name)
        resource = await loop.run_in_executor(None, lambda: self._az_get(url, timeout=30))

        if resource is None:
            return DeploymentResult(
                success=False,
                error_message="Post-deployment GET verification failed.",
            )

        state = resource.get("properties", {}).get("state", "Enabled")
        if state.lower() in ("enabled", ""):
            return DeploymentResult(success=True)

        return DeploymentResult(
            success=False,
            error_message=f"Deployed workflow is in unexpected state: '{state}'.",
        )




# """
# Deployment Engine Module
# Deploys Azure Standard Logic App workflows via az CLI.

# SAFETY FIRST:
# - Pre-deployment check lists ALL existing workflows on the Logic App
# - Only deploys if the workflow is NEW or explicitly allowed to update
# - Never deletes or replaces existing workflows unless --allow-update flag is set
# - Prints a full pre-flight summary before any changes are made
# """
# import asyncio
# import hashlib
# import json
# import re
# import subprocess
# import time
# from dataclasses import dataclass, field
# from pathlib import Path
# from typing import Optional, List, Dict

# from utils.logger import get_logger

# logger = get_logger("deployer")


# @dataclass
# class DeploymentResult:
#     success: bool
#     error_message: str = ""
#     raw_stdout: str = ""
#     raw_stderr: str = ""
#     correlation_id: str = ""
#     skipped: bool = False
#     skip_reason: str = ""


# @dataclass
# class PreflightReport:
#     """Summary of what WILL and WON'T be touched before deployment runs."""
#     workflow_name: str
#     action: str          # "CREATE", "UPDATE", "SKIP"
#     reason: str
#     existing_definition_hash: str = ""
#     new_definition_hash: str = ""
#     existing_workflows_on_app: List[str] = field(default_factory=list)


# class DeploymentEngine:
#     def __init__(self, config):
#         self.config = config
#         self._verify_az_cli()

#     # ── az CLI helpers ────────────────────────────────────────────────────────

#     def _verify_az_cli(self):
#         """Verify az CLI is available and the session is authenticated."""
#         az = self.config.az_cli_path
#         try:
#             result = subprocess.run(
#                 [az, "account", "show", "--output", "json"],
#                 capture_output=True, text=True, timeout=30,
#                 shell=True
#             )
#             if result.returncode != 0:
#                 raise RuntimeError(
#                     f"az CLI is not authenticated.\n"
#                     f"Run 'az login' first.\nstderr: {result.stderr[:300]}"
#                 )
#             account = json.loads(result.stdout)
#             logger.info("az CLI verified", extra={
#                 "subscription_id": account.get("id"),
#                 "user": account.get("user", {}).get("name"),
#             })
#         except FileNotFoundError:
#             raise RuntimeError(f"az CLI not found at '{az}'. Install Azure CLI and add it to PATH.")

#     def _build_arm_url(self, workflow_name: str) -> str:
#         sub = self.config.subscription_id
#         rg  = self.config.resource_group
#         app = self.config.logic_app_name
#         api = self.config.arm_api_version
#         return (
#             f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
#             f"/providers/Microsoft.Web/sites/{app}/workflows/{workflow_name}"
#             f"?api-version={api}"
#         )

#     def _build_list_url(self) -> str:
#         """URL to list ALL workflows on the Logic App."""
#         sub = self.config.subscription_id
#         rg  = self.config.resource_group
#         app = self.config.logic_app_name
#         api = self.config.arm_api_version
#         return (
#             f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
#             f"/providers/Microsoft.Web/sites/{app}/workflows"
#             f"?api-version={api}"
#         )

#     def _az_get(self, url: str, timeout: int = 30) -> Optional[dict]:
#         """Run az rest GET and return parsed JSON, or None on failure."""
#         az = self.config.az_cli_path
#         result = subprocess.run(
#             [az, "rest", "--method", "GET", "--url", url, "--output", "json"],
#             capture_output=True, text=True, timeout=timeout, shell=True
#         )
#         if result.returncode != 0:
#             return None
#         try:
#             return json.loads(result.stdout)
#         except json.JSONDecodeError:
#             return None

#     def _definition_hash(self, workflow_json: dict) -> str:
#         """SHA256 hash of the workflow definition for change detection."""
#         definition = workflow_json.get("definition", workflow_json)
#         canonical = json.dumps(definition, sort_keys=True)
#         return hashlib.sha256(canonical.encode()).hexdigest()[:16]

#     def _extract_error_message(self, stdout: str, stderr: str, returncode: int) -> str:
#         for candidate in [stdout, stderr]:
#             if not candidate.strip():
#                 continue
#             try:
#                 data = json.loads(candidate)
#                 if "error" in data:
#                     err = data["error"]
#                     code = err.get("code", "")
#                     message = err.get("message", "")
#                     details = err.get("details", [])
#                     detail_msgs = "; ".join(d.get("message", "") for d in details if "message" in d)
#                     full = f"[{code}] {message}"
#                     if detail_msgs:
#                         full += f" | Details: {detail_msgs}"
#                     return full
#             except (json.JSONDecodeError, TypeError):
#                 pass
#         combined = (stderr or stdout or "").strip()
#         return combined[:1000] if combined else f"Deployment failed with exit code {returncode}"

#     def _parse_correlation_id(self, output: str) -> str:
#         match = re.search(r'"x-ms-correlation-request-id":\s*"([^"]+)"', output, re.IGNORECASE)
#         return match.group(1) if match else ""

#     def _exponential_backoff(self, attempt: int):
#         import random
#         base = self.config.retry_backoff_base_seconds
#         max_wait = self.config.retry_backoff_max_seconds
#         wait = min(base * (2 ** (attempt - 1)), max_wait)
#         jitter = random.uniform(0, wait * 0.1)
#         total = wait + jitter
#         logger.info(f"Back-off: waiting {total:.1f}s before next attempt")
#         time.sleep(total)

#     # ── Pre-flight safety check ───────────────────────────────────────────────

#     async def preflight_check(
#         self,
#         workflow_json: dict,
#         workflow_name: str,
#         allow_update: bool = False,
#     ) -> PreflightReport:
#         """
#         Check what exists on the Logic App BEFORE making any changes.

#         Rules:
#           - Lists all existing workflows on the Logic App.
#           - If workflow_name does NOT exist → action = CREATE (safe, no existing resource touched).
#           - If workflow_name EXISTS and allow_update=False → action = SKIP (protect existing).
#           - If workflow_name EXISTS and allow_update=True and definition is UNCHANGED → action = SKIP (no-op).
#           - If workflow_name EXISTS and allow_update=True and definition CHANGED → action = UPDATE.
#         """
#         loop = asyncio.get_event_loop()

#         # ── List all workflows on this Logic App ──────────────────
#         list_url = self._build_list_url()
#         logger.info("Pre-flight: listing all existing workflows on Logic App",
#                     extra={"logic_app": self.config.logic_app_name})

#         list_response = await loop.run_in_executor(None, lambda: self._az_get(list_url))

#         existing_workflows: List[str] = []
#         if list_response and "value" in list_response:
#             existing_workflows = [
#                 w.get("name", "").split("/")[-1]   # name can be "app/workflowname"
#                 for w in list_response["value"]
#             ]

#         logger.info(
#             f"Pre-flight: found {len(existing_workflows)} existing workflow(s)",
#             extra={"workflows": existing_workflows}
#         )

#         new_hash = self._definition_hash(workflow_json)

#         # ── Workflow does NOT exist yet → safe CREATE ─────────────
#         if workflow_name not in existing_workflows:
#             report = PreflightReport(
#                 workflow_name=workflow_name,
#                 action="CREATE",
#                 reason="Workflow does not exist on the Logic App — safe to create.",
#                 new_definition_hash=new_hash,
#                 existing_workflows_on_app=existing_workflows,
#             )
#             self._print_preflight_summary(report)
#             return report

#         # ── Workflow EXISTS ───────────────────────────────────────
#         if not allow_update:
#             report = PreflightReport(
#                 workflow_name=workflow_name,
#                 action="SKIP",
#                 reason=(
#                     f"Workflow '{workflow_name}' already exists and --allow-update is NOT set. "
#                     f"Skipping to protect existing resource. "
#                     f"Pass --allow-update to overwrite."
#                 ),
#                 new_definition_hash=new_hash,
#                 existing_workflows_on_app=existing_workflows,
#             )
#             self._print_preflight_summary(report)
#             return report

#         # allow_update=True — check if definition actually changed
#         get_url = self._build_arm_url(workflow_name)
#         existing_resource = await loop.run_in_executor(None, lambda: self._az_get(get_url))

#         existing_hash = ""
#         if existing_resource:
#             existing_definition = existing_resource.get("properties", {})
#             existing_hash = self._definition_hash(existing_definition)

#         if existing_hash == new_hash:
#             report = PreflightReport(
#                 workflow_name=workflow_name,
#                 action="SKIP",
#                 reason="Workflow definition is identical to what is already deployed — no changes needed.",
#                 existing_definition_hash=existing_hash,
#                 new_definition_hash=new_hash,
#                 existing_workflows_on_app=existing_workflows,
#             )
#             self._print_preflight_summary(report)
#             return report

#         report = PreflightReport(
#             workflow_name=workflow_name,
#             action="UPDATE",
#             reason=f"Workflow exists and --allow-update is set. Definition has changed (hash: {existing_hash} → {new_hash}).",
#             existing_definition_hash=existing_hash,
#             new_definition_hash=new_hash,
#             existing_workflows_on_app=existing_workflows,
#         )
#         self._print_preflight_summary(report)
#         return report

#     def _print_preflight_summary(self, report: PreflightReport):
#         """Print a human-readable pre-flight summary to stdout."""
#         divider = "=" * 65
#         print(f"\n{divider}")
#         print(f"  PRE-FLIGHT DEPLOYMENT SUMMARY")
#         print(divider)
#         print(f"  Logic App  : {self.config.logic_app_name}")
#         print(f"  Resource Grp: {self.config.resource_group}")
#         print(f"  Workflow    : {report.workflow_name}")
#         print(f"  Action      : {report.action}")
#         print(f"  Reason      : {report.reason}")
#         print(f"\n  All existing workflows on this Logic App ({len(report.existing_workflows_on_app)}):")
#         if report.existing_workflows_on_app:
#             for wf in report.existing_workflows_on_app:
#                 marker = "  ← TARGET (will be updated)" if wf == report.workflow_name and report.action == "UPDATE" else ""
#                 print(f"    • {wf}{marker}")
#         else:
#             print("    (none found)")
#         print(divider)

#         if report.action == "SKIP":
#             print(f"\n  ⏭  SKIPPED — No changes will be made to Azure.\n")
#         elif report.action == "CREATE":
#             print(f"\n  ✅ SAFE CREATE — A brand-new workflow will be created.\n")
#         elif report.action == "UPDATE":
#             print(f"\n  ⚠️  UPDATE — Existing workflow WILL be overwritten.\n")
#         print()

#     # ── Deployment ────────────────────────────────────────────────────────────

#     async def deploy(
#         self,
#         workflow_json: dict,
#         workflow_name: str,
#         workflow_file_path: Path,
#         allow_update: bool = False,
#     ) -> DeploymentResult:
#         """
#         Safe deployment with pre-flight check.

#         Steps:
#           1. Pre-flight: list existing workflows, determine action (CREATE / UPDATE / SKIP).
#           2. If SKIP → return success immediately without touching Azure.
#           3. If CREATE or UPDATE → proceed with az rest PUT.
#           4. Post-deployment verification via GET.
#         """
#         # ── Step 1: Pre-flight ────────────────────────────────────
#         report = await self.preflight_check(workflow_json, workflow_name, allow_update)

#         if report.action == "SKIP":
#             return DeploymentResult(
#                 success=True,
#                 skipped=True,
#                 skip_reason=report.reason,
#             )

#         # ── Step 2: Build and write ARM payload ───────────────────
#         arm_payload = {
#             "properties": {
#                 "definition": workflow_json.get("definition", {}),
#                 "parameters": workflow_json.get("parameters", {}),
#             }
#         }
#         payload_path = workflow_file_path.parent / f"{workflow_file_path.stem}_arm_payload.json"

#         try:
#             with open(payload_path, "w", encoding="utf-8") as f:
#                 json.dump(arm_payload, f, indent=2)

#             az  = self.config.az_cli_path
#             url = self._build_arm_url(workflow_name)

#             cmd = [
#                 az, "rest",
#                 "--method", "PUT",
#                 "--url", url,
#                 "--body", f"@{payload_path}",
#                 "--output", "json",
#                 "--verbose",
#             ]

#             logger.info("Executing deployment", extra={
#                 "action": report.action,
#                 "workflow_name": workflow_name,
#             })

#             loop = asyncio.get_event_loop()
#             proc = await loop.run_in_executor(
#                 None,
#                 lambda: subprocess.run(
#                     cmd,
#                     capture_output=True,
#                     text=True,
#                     timeout=self.config.az_deployment_timeout_seconds,
#                 )
#             )

#             correlation_id = self._parse_correlation_id(proc.stderr)

#             if proc.returncode == 0:
#                 verify = await self._verify_deployment(workflow_name)
#                 if verify.success:
#                     print(f"\n  ✅ Deployment SUCCESS — '{workflow_name}' is live on Azure.\n")
#                     return DeploymentResult(
#                         success=True,
#                         raw_stdout=proc.stdout,
#                         raw_stderr=proc.stderr,
#                         correlation_id=correlation_id,
#                     )
#                 return verify

#             error_msg = self._extract_error_message(proc.stdout, proc.stderr, proc.returncode)
#             logger.warning("Deployment failed", extra={"error": error_msg[:300]})
#             return DeploymentResult(
#                 success=False,
#                 error_message=error_msg,
#                 raw_stdout=proc.stdout,
#                 raw_stderr=proc.stderr,
#                 correlation_id=correlation_id,
#             )

#         except subprocess.TimeoutExpired:
#             return DeploymentResult(
#                 success=False,
#                 error_message=f"Deployment timed out after {self.config.az_deployment_timeout_seconds}s",
#             )
#         except Exception as e:
#             return DeploymentResult(
#                 success=False,
#                 error_message=f"Unexpected deployment error: {str(e)}",
#             )
#         finally:
#             if payload_path.exists():
#                 try:
#                     payload_path.unlink()
#                 except OSError:
#                     pass

#     async def _verify_deployment(self, workflow_name: str) -> DeploymentResult:
#         """GET the resource after PUT to confirm it is in Enabled state."""
#         loop = asyncio.get_event_loop()
#         url = self._build_arm_url(workflow_name)
#         resource = await loop.run_in_executor(None, lambda: self._az_get(url, timeout=30))

#         if resource is None:
#             return DeploymentResult(
#                 success=False,
#                 error_message="Post-deployment GET verification failed — could not retrieve deployed resource.",
#             )

#         state = resource.get("properties", {}).get("state", "Enabled")
#         if state.lower() in ("enabled", ""):
#             return DeploymentResult(success=True)

#         return DeploymentResult(
#             success=False,
#             error_message=f"Deployed workflow is in unexpected state: '{state}'. Expected 'Enabled'.",
#         )
