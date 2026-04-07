"""
AI-Driven Azure Logic App Automation System
============================================
Orchestrates: Prompt → Generate → Validate → Persist → Deploy (self-healing loop)

Safety flags:
  --dry-run       Generate + validate + save locally. No Azure deployment.
  --allow-update  Allow overwriting an existing workflow. Without this flag,
                  existing workflows are NEVER touched.
"""
import sys
import argparse
import asyncio
from pathlib import Path

from utils.logger import get_logger
from utils.config import Config
from generator.workflow_generator import WorkflowGenerator
from validator.schema_validator import SchemaValidator
from deployer.deployment_engine import DeploymentEngine
from utils.file_writer import FileWriter

logger = get_logger("main")


async def run_pipeline(
    prompt: str,
    workflow_name: str,
    dry_run: bool = False,
    allow_update: bool = False,
) -> dict:
    config = Config.load()
    logger.info("Pipeline started", extra={
        "workflow_name": workflow_name,
        "dry_run": dry_run,
        "allow_update": allow_update,
    })

    generator   = WorkflowGenerator(config)
    validator   = SchemaValidator(config)
    file_writer = FileWriter(config)
    deployer    = DeploymentEngine(config)

    result = {
        "success": False,
        "workflow_name": workflow_name,
        "output_path": None,
        "attempts": 0,
        "skipped": False,
        "skip_reason": "",
        "final_error": None,
    }

    # ── Stage 1: Initial AI Generation ───────────────────────────
    print("\n[1/4] Generating workflow definition via AI...")
    workflow_json = await generator.generate(prompt, workflow_name)
    logger.info("Workflow JSON generated", extra={"size_bytes": len(str(workflow_json))})

    previous_errors = []

    for attempt in range(1, config.max_retries + 1):
        result["attempts"] = attempt
        logger.info(f"Attempt {attempt}/{config.max_retries}")

        # ── Stage 2: Schema Validation ────────────────────────────
        print(f"\n[2/4] Validating schema (attempt {attempt}/{config.max_retries})...")
        validation_result = validator.validate(workflow_json)

        if not validation_result.is_valid:
            logger.warning("Schema validation failed", extra={"errors": validation_result.errors})
            print(f"  ❌ Validation failed ({len(validation_result.errors)} error(s)):")
            for e in validation_result.errors:
                print(f"     • {e}")

            if attempt == config.max_retries:
                result["final_error"] = f"Schema validation failed after {attempt} attempts."
                file_writer.save_failed(workflow_json, workflow_name, attempt, validation_result.errors)
                return result

            print(f"\n  🔁 Sending errors to AI for correction...")
            workflow_json = await generator.correct(
                workflow_json, validation_result.errors, previous_errors
            )
            previous_errors.extend(validation_result.errors)
            continue

        print(f"  ✅ Schema validation passed.")

        # ── Stage 3: Local Persistence ────────────────────────────
        print(f"\n[3/4] Saving workflow to local folder...")
        output_path = file_writer.save(workflow_json, workflow_name, attempt)
        result["output_path"] = str(output_path)
        print(f"  ✅ Saved: {output_path}")

        if dry_run:
            print(f"\n[4/4] Dry-run mode — skipping Azure deployment.")
            result["success"] = True
            return result

        # ── Stage 4: Deployment & Self-Healing Loop ───────────────
        print(f"\n[4/4] Deploying to Azure...")
        deploy_result = await deployer.deploy(
            workflow_json,
            workflow_name,
            output_path,
            allow_update=allow_update,
        )

        if deploy_result.skipped:
            result["success"] = True
            result["skipped"] = True
            result["skip_reason"] = deploy_result.skip_reason
            return result

        if deploy_result.success:
            result["success"] = True
            return result

        print(f"  ❌ Deployment failed: {deploy_result.error_message[:200]}")
        logger.warning("Deployment failed", extra={"attempt": attempt, "error": deploy_result.error_message})

        if attempt == config.max_retries:
            result["final_error"] = f"Deployment failed after {attempt} attempts: {deploy_result.error_message}"
            file_writer.save_failed(workflow_json, workflow_name, attempt, [deploy_result.error_message])
            return result

        print(f"\n  🔁 Sending deployment error to AI for correction...")
        workflow_json = await generator.correct(
            workflow_json, [deploy_result.error_message], previous_errors
        )
        previous_errors.append(deploy_result.error_message)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="AI-Driven Azure Logic App Automation System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Safety behaviour:
  By default, existing workflows on the Logic App are NEVER modified or deleted.
  The system will SKIP deployment if the workflow already exists.

  Use --allow-update only when you explicitly want to overwrite an existing workflow.

Examples:
  # Generate + validate locally only (no Azure)
  python main.py --name my-workflow --prompt "..." --dry-run

  # Deploy new workflow (will SKIP if workflow already exists)
  python main.py --name my-workflow --prompt "..."

  # Deploy and allow updating an existing workflow
  python main.py --name my-workflow --prompt "..." --allow-update
        """
    )
    prompt_group = parser.add_mutually_exclusive_group(required=True)
    prompt_group.add_argument("--prompt", help="Natural language workflow description")
    prompt_group.add_argument("--prompt-file", help="Path to a file containing the natural language workflow description")
    parser.add_argument("--name",          required=True, help="Workflow name (alphanumeric, hyphens only)")
    parser.add_argument("--dry-run",       action="store_true", help="Skip Azure deployment entirely")
    parser.add_argument("--allow-update",  action="store_true",
                        help="Allow overwriting an existing workflow. Without this, existing workflows are protected.")
    args = parser.parse_args()

    prompt = args.prompt
    if args.prompt_file:
        prompt_path = Path(args.prompt_file)
        if not prompt_path.exists():
            parser.error(f"Prompt file not found: {args.prompt_file}")
        prompt = prompt_path.read_text(encoding="utf-8")

    result = asyncio.run(run_pipeline(
        prompt,
        args.name,
        dry_run=args.dry_run,
        allow_update=args.allow_update,
    ))

    print("\n" + "=" * 65)
    if result["success"]:
        if result.get("skipped"):
            print(f"  ⏭  SKIPPED after {result['attempts']} attempt(s).")
            print(f"  Reason : {result['skip_reason']}")
        else:
            print(f"  ✅ SUCCESS after {result['attempts']} attempt(s).")
            if result["output_path"]:
                print(f"  Saved  : {result['output_path']}")
        print("=" * 65 + "\n")
        sys.exit(0)
    else:
        print(f"  ❌ FAILED after {result['attempts']} attempt(s).")
        print(f"  Error  : {result['final_error']}")
        print("=" * 65 + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
