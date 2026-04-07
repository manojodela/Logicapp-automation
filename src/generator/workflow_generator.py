"""
AI Workflow Generator
Calls the Groq API (free) to generate and correct Azure Standard Logic App ARM JSON definitions.
Get your free API key at: https://console.groq.com
"""
import json
import re
import asyncio
import aiohttp
from typing import Any

from utils.logger import get_logger

logger = get_logger("generator")

GENERATION_SYSTEM_PROMPT = """
You are an expert Azure Integration architect specialized in Azure Standard Logic Apps (single-tenant).
Your task is to generate a valid Azure ARM JSON workflow definition based on the user's requirement.

=== MANDATORY SCHEMA STRICTNESS ===
You MUST strictly adhere to the Azure Logic Apps workflowdefinition.json (2016-06-01) schema.
DO NOT hallucinate properties. If a property is not in the standard 2016-06-01 schema, do not invent it.

1. TOP LEVEL:
The JSON MUST contain EXACTLY these three top-level keys: "definition", "kind", and "parameters".
The "kind" key MUST be set to "Stateful".
Inside "definition", you MUST include EXACTLY: "$schema", "contentVersion", "triggers", "actions", and "outputs".

2. TRIGGERS:
- For HTTP requests, use `"type": "Request"` and `"kind": "Http"`. DO NOT use `"type": "Http"` for the trigger unless it is an outbound polling trigger.
- Do NOT put "recurrence" inside an HTTP Request trigger.

3. ACTIONS (CRITICAL RULES):
- Every action inside the "actions" dictionary MUST have a "runAfter" object specifying the exact, case-sensitive name of the preceding action.
- CONDITIONALS: To evaluate logic, use `"type": "If"`. You MUST use an `"expression"` object containing a logical tree (e.g., `{"and": [ {"greaterOrEquals": [...] } ] }`). DO NOT invent a "conditions" array.
- RESPONSES: To return an HTTP response, you MUST use an action with `"type": "Response"` and `"kind": "Http"` placed inside the "actions" block. DO NOT place responses in the global "outputs" block.
- API CONNECTIONS: Use `"type": "ApiConnection"`.

=== REQUIRED OUTPUT SKELETON ===
Your output MUST perfectly map to this exact structural skeleton. Fill in the {...} appropriately.

{
  "definition": {
    "$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
    "contentVersion": "1.0.0.0",
    "triggers": {
      "YourTriggerName": { ... }
    },
    "actions": {
      "YourFirstActionName": { 
         "runAfter": {}
      },
      "YourSecondActionName": {
         "runAfter": { "YourFirstActionName": ["Succeeded"] }
      }
    },
    "outputs": {}
  },
  "kind": "Stateful",
  "parameters": {
    "$connections": {
      "value": { ... }
    }
  }
}

RESPOND WITH RAW JSON ONLY — no preamble, no postamble, no markdown code fences.
""".strip()


class WorkflowGenerator:
    def __init__(self, config):
        self.config = config

    async def _call_llm(self, system_prompt: str, user_message: str) -> str:
        """Make a raw API call to the Groq API and return the response text."""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config.llm_api_key}",
        }
        payload = {
            "model": self.config.llm_model,
            "max_tokens": self.config.llm_max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.1,
        }

        timeout = aiohttp.ClientTimeout(total=self.config.llm_timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self.config.llm_api_url,
                headers=headers,
                json=payload,
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    raise RuntimeError(
                        f"Groq API error {response.status}: {body[:500]}"
                    )
                data = await response.json()

        # Groq uses OpenAI-compatible response format
        return data["choices"][0]["message"]["content"].strip()

    def _repair_json(self, text: str) -> str:
        """
        Attempt to repair common JSON issues like unterminated strings.
        """
        # Try to find and fix unterminated strings by closing them
        result = []
        in_string = False
        escape_next = False
        
        for i, char in enumerate(text):
            if escape_next:
                result.append(char)
                escape_next = False
                continue
            
            if char == '\\' and in_string:
                result.append(char)
                escape_next = True
                continue
            
            if char == '"':
                in_string = not in_string
                result.append(char)
            else:
                result.append(char)
        
        # If still in a string at the end, close it
        if in_string:
            result.append('"')
        
        return ''.join(result)

    def _parse_json_response(self, raw_text: str) -> dict:
        """
        Parse JSON from LLM response text.
        Handles cases where the model wraps output in markdown code fences or returns malformed JSON.
        Implements progressive repair strategies for common JSON issues.
        """
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned.strip())

        # Strategy 1: Try direct parse
        try:
            parsed = json.loads(cleaned)
            # If successfully parsed and has "definition", return it (even if it has extra fields)
            if isinstance(parsed, dict) and "definition" in parsed:
                return {
                    "definition": parsed["definition"],
                    "parameters": parsed.get("parameters", {})
                }
            return parsed
        except json.JSONDecodeError as e:
            logger.debug(f"Strategy 1 (direct parse) failed: {str(e)}")
        
        # Strategy 2: Try to find JSON object boundaries and extract
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if match:
            json_candidate = match.group(0)
            try:
                parsed = json.loads(json_candidate)
                # If successfully parsed and has "definition", return it
                if isinstance(parsed, dict) and "definition" in parsed:
                    return {
                        "definition": parsed["definition"],
                        "parameters": parsed.get("parameters", {})
                    }
                return parsed
            except json.JSONDecodeError as e:
                logger.debug(f"Strategy 2 (regex extract) failed: {str(e)}")
        
        # Strategy 3: Try to repair unterminated strings and common issues
        repaired = self._repair_json(cleaned)
        try:
            parsed = json.loads(repaired)
            if isinstance(parsed, dict) and "definition" in parsed:
                return {
                    "definition": parsed["definition"],
                    "parameters": parsed.get("parameters", {})
                }
            return parsed
        except json.JSONDecodeError as e:
            logger.debug(f"Strategy 3 (repair unterminated strings) failed: {str(e)}")
        
        # Strategy 4: Try to repair the extracted JSON object
        if match:
            repaired_match = self._repair_json(match.group(0))
            try:
                parsed = json.loads(repaired_match)
                if isinstance(parsed, dict) and "definition" in parsed:
                    return {
                        "definition": parsed["definition"],
                        "parameters": parsed.get("parameters", {})
                    }
                return parsed
            except json.JSONDecodeError as e:
                logger.debug(f"Strategy 4 (repair + extract) failed: {str(e)}")
        
        # Strategy 5: As a last resort, try to truncate at the last valid closing brace
        braces = 0
        last_valid_pos = -1
        in_string = False
        escape_next = False
        
        for i, char in enumerate(cleaned):
            if escape_next:
                escape_next = False
                continue
            if char == '\\':
                escape_next = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if not in_string:
                if char == '{':
                    braces += 1
                elif char == '}':
                    braces -= 1
                    if braces == 0:
                        last_valid_pos = i + 1
        
        if last_valid_pos > 0:
            truncated = cleaned[:last_valid_pos]
            try:
                parsed = json.loads(truncated)
                if isinstance(parsed, dict) and "definition" in parsed:
                    return {
                        "definition": parsed["definition"],
                        "parameters": parsed.get("parameters", {})
                    }
                return parsed
            except json.JSONDecodeError as e:
                logger.debug(f"Strategy 5 (truncate at brace) failed: {str(e)}")
        
        raise ValueError(
            f"Groq returned non-parseable JSON after all repair attempts. "
            f"Raw (first 500 chars): {raw_text[:500]}"
        )

    async def generate(self, prompt: str, workflow_name: str) -> dict:
        """Generate an Azure Standard Logic App workflow definition from a natural language prompt."""
        user_message = f"""
Generate an Azure Standard Logic App workflow definition for the following requirement:

Workflow Name: {workflow_name}
Description: {prompt}

Return the complete workflow definition JSON object with "definition" and "parameters" keys.
""".strip()

        logger.info("Calling Groq for initial workflow generation", extra={"workflow_name": workflow_name})
        raw_response = await self._call_llm(GENERATION_SYSTEM_PROMPT, user_message)
        logger.debug("Groq raw generation response received", extra={"length": len(raw_response), "first_200_chars": raw_response[:200]})

        workflow_json = self._parse_json_response(raw_response)
        logger.info("Workflow JSON successfully parsed from Groq response")
        return workflow_json

    async def correct(self, workflow_json: dict, current_errors: list, previous_errors: list) -> dict:
        """Ask the LLM to fix a workflow definition given a list of errors."""
        user_message = json.dumps(
            {
                "current_errors": current_errors,
                "previous_errors": previous_errors,
                "workflow_to_fix": workflow_json,
            },
            indent=2,
        )

        logger.info(
            "Calling Groq for workflow correction",
            extra={"current_error_count": len(current_errors)},
        )
        raw_response = await self._call_llm(CORRECTION_SYSTEM_PROMPT, user_message)
        logger.debug("Groq correction response received", extra={"length": len(raw_response), "first_200_chars": raw_response[:200]})
        corrected_json = self._parse_json_response(raw_response)
        logger.info("Corrected workflow JSON successfully parsed")
        return corrected_json
