# """
# AI Workflow Generator
# Calls the Groq API (free) to generate and correct Azure Standard Logic App ARM JSON definitions.
# Get your free API key at: https://console.groq.com
# """
# import json
# import re
# import asyncio
# import aiohttp
# from typing import Any

# from utils.logger import get_logger

# logger = get_logger("generator")

# GENERATION_SYSTEM_PROMPT = """
# You are an expert Azure Integration architect specialised in Azure Standard Logic Apps (single-tenant).

# Your task is to generate a valid Azure ARM JSON workflow definition for an Azure Standard Logic App.

# STRICT REQUIREMENTS:
#1. The output MUST be a valid JSON object conforming to the Azure Standard Logic App workflow definition schema.
# 2. The top-level structure MUST include:
#    - "definition": object containing "$schema", "contentVersion", "triggers", "actions", "outputs"
#    - "parameters": object (can be empty {})
# 3. The "$schema" inside "definition" MUST EXACTLY match: "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#"
# 4. The "contentVersion" inside "definition" MUST be "1.0.0.0".
# 5. All trigger and action types must use fully qualified connector references (e.g., "type": "Http", "type": "ApiConnection").
# 6. Do NOT include any ARM resource wrapper — return ONLY the workflow content object (definition + parameters).
# 7. Do NOT include markdown code fences, explanations, or any text outside the JSON object.
# 8. Ensure all required fields for each action/trigger type are present and correctly typed.
# 9. Use realistic, production-appropriate connector configurations.

# RESPOND WITH JSON ONLY — no preamble, no postamble, no markdown.
# """.strip()

# CORRECTION_SYSTEM_PROMPT = """
# You are an expert Azure Integration architect specialised in Azure Standard Logic Apps (single-tenant).

# You are given an Azure Standard Logic App workflow definition JSON that contains errors.
# Your task is to fix ALL reported errors and return a corrected, valid JSON definition.

# STRICT REQUIREMENTS:
# 1. Fix every error listed in the "current_errors" array.
# 2. Do NOT re-introduce errors from the "previous_errors" array.
# 3. The output MUST conform to the Azure Standard Logic App workflow definition schema, specifically using "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#" as the $schema.
# 4. Do NOT include markdown code fences, explanations, or any text outside the JSON object.
# 5. Return the COMPLETE corrected workflow JSON — not just the changed parts.

# RESPOND WITH JSON ONLY — no preamble, no postamble, no markdown.
# """.strip()


# class WorkflowGenerator:
#     def __init__(self, config):
#         self.config = config

#     # async def _call_llm(self, system_prompt: str, user_message: str) -> str:
#     #     """Make a raw API call to the Groq API and return the response text."""
#     #     headers = {
#     #         "Content-Type": "application/json",
#     #         "Authorization": f"Bearer {self.config.llm_api_key}",
#     #     }
#     #     payload = {
#     #         "model": self.config.llm_model,
#     #         "max_tokens": self.config.llm_max_tokens,
#     #         "messages": [
#     #             {"role": "system", "content": system_prompt},
#     #             {"role": "user",   "content": user_message},
#     #         ],
#     #         "temperature": 0.1,
#     #     }

#     #     timeout = aiohttp.ClientTimeout(total=self.config.llm_timeout_seconds)

#     #     async with aiohttp.ClientSession(timeout=timeout) as session:
#     #         async with session.post(
#     #             self.config.llm_api_url,
#     #             headers=headers,
#     #             json=payload,
#     #         ) as response:
#     #             if response.status != 200:
#     #                 body = await response.text()
#     #                 raise RuntimeError(
#     #                     f"Groq API error {response.status}: {body[:500]}"
#     #                 )
#     #             data = await response.json()

#     #     # Groq uses OpenAI-compatible response format
#     #     return data["choices"][0]["message"]["content"].strip()
    
#     async def _call_llm(self, system_prompt: str, user_message: str) -> str:
#         """Make a raw API call to the Groq API and return the response text, with built-in retry for rate limits."""
#         headers = {
#             "Content-Type": "application/json",
#             "Authorization": f"Bearer {self.config.llm_api_key}",
#         }
#         payload = {
#             "model": self.config.llm_model,
#             "max_tokens": self.config.llm_max_tokens,
#             "messages": [
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user",   "content": user_message},
#             ],
#             "temperature": 0.1,
#         }

#         timeout = aiohttp.ClientTimeout(total=self.config.llm_timeout_seconds)
#         max_retries = 3
#         wait_time = 6  # Start by waiting 6 seconds (to cover the 5.17s requested by Groq)

#         async with aiohttp.ClientSession(timeout=timeout) as session:
#             for attempt in range(1, max_retries + 1):
#                 async with session.post(
#                     self.config.llm_api_url,
#                     headers=headers,
#                     json=payload,
#                 ) as response:
                    
#                     # Handle Rate Limits (HTTP 429) gracefully
#                     if response.status == 429:
#                         if attempt < max_retries:
#                             logger.warning(
#                                 f"Groq Rate Limit hit (429). Attempt {attempt}/{max_retries}. "
#                                 f"Waiting {wait_time} seconds before retrying..."
#                             )
#                             await asyncio.sleep(wait_time)
#                             wait_time *= 2  # Exponential backoff (6s -> 12s -> 24s)
#                             continue
#                         else:
#                             body = await response.text()
#                             raise RuntimeError(
#                                 f"Groq API error 429: Max retries exceeded. {body[:500]}"
#                             )

#                     # Handle all other non-200 errors by crashing (normal behavior)
#                     if response.status != 200:
#                         body = await response.text()
#                         raise RuntimeError(
#                             f"Groq API error {response.status}: {body[:500]}"
#                         )
                        
#                     data = await response.json()
#                     # Groq uses OpenAI-compatible response format
#                     return data["choices"][0]["message"]["content"].strip()

#     def _parse_json_response(self, raw_text: str) -> dict:
#         """
#         Parse JSON from LLM response text.
#         Handles cases where the model wraps output in markdown code fences despite instructions.
#         Auto-corrects missing 'definition' wrappers to save API tokens.
#         """
        
#         cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.IGNORECASE)
#         cleaned = re.sub(r"\s*```$", "", cleaned.strip())

#         try:
#             return json.loads(cleaned)
#         except json.JSONDecodeError as e:
#             match = re.search(r"\{.*\}", cleaned, re.DOTALL)
#             if match:
#                 try:
#                     return json.loads(match.group(0))
#                 except json.JSONDecodeError:
#                     pass
#             raise ValueError(
#                 f"Groq returned non-parseable JSON: {str(e)}\nRaw (first 500 chars): {raw_text[:500]}"
#             )

#     async def generate(self, prompt: str, workflow_name: str) -> dict:
#         """Generate an Azure Standard Logic App workflow definition from a natural language prompt."""
#         user_message = f"""
#         Generate an Azure Standard Logic App workflow definition for the following requirement:

#         Workflow Name: {workflow_name}
#         Description: {prompt}

#         Return the complete workflow definition JSON object with "definition" and "parameters" keys.
#         """.strip()

#         logger.info("Calling Groq for initial workflow generation", extra={"workflow_name": workflow_name})
#         raw_response = await self._call_llm(GENERATION_SYSTEM_PROMPT, user_message)
#         logger.debug("Groq raw generation response received", extra={"length": len(raw_response)})

#         workflow_json = self._parse_json_response(raw_response)
#         logger.info("Workflow JSON successfully parsed from Groq response")
#         return workflow_json

#     async def correct(self, workflow_json: dict, current_errors: list, previous_errors: list) -> dict:
#         """Ask the LLM to fix a workflow definition given a list of errors."""
#         user_message = json.dumps(
#             {
#                 "current_errors": current_errors,
#                 "previous_errors": previous_errors,
#                 "workflow_to_fix": workflow_json,
#             },
#             indent=2,
#         )

#         logger.info(
#             "Calling Groq for workflow correction",
#             extra={"current_error_count": len(current_errors)},
#         )
#         raw_response = await self._call_llm(CORRECTION_SYSTEM_PROMPT, user_message)
#         corrected_json = self._parse_json_response(raw_response)
#         logger.info("Corrected workflow JSON successfully parsed")
#         return corrected_json


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
You are an expert Azure Integration architect specialised in Azure Standard Logic Apps (single-tenant).

Your task is to generate a valid Azure ARM JSON workflow definition for an Azure Standard Logic App.

STRICT REQUIREMENTS:
1. The output MUST be a valid JSON object conforming to the Azure Standard Logic App ARM schema at API Version 2016-06-01.
2. The top-level structure MUST ONLY include EXACTLY TWO keys:
   - "definition": object containing "$schema", "contentVersion", "triggers", "actions", "outputs"
   - "parameters": object (can be empty {})
   Do NOT include any other top-level keys like "kind", "name", "metadata", "properties", etc.
3. The "$schema" inside "definition" MUST be:
   "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#"
4. The "contentVersion" inside "definition" MUST be "1.0.0.0".
5. All trigger and action types must use fully qualified connector references (e.g., "type": "Http", "type": "ApiConnection").
6. Output ONLY a {definition, parameters} object — do NOT wrap it in any other structure or include extra fields.
7. Do NOT include markdown code fences, explanations, or any text outside the JSON object.
8. Ensure all required fields for each action/trigger type are present and correctly typed.
9. Use realistic, production-appropriate connector configurations.

RESPOND WITH JSON ONLY — no preamble, no postamble, no markdown.
""".strip()

CORRECTION_SYSTEM_PROMPT = """
You are an expert Azure Integration architect specialised in Azure Standard Logic Apps (single-tenant).

You are given an Azure Standard Logic App workflow definition JSON that contains errors.
Your task is to fix ALL reported errors and return a corrected, valid JSON definition.

STRICT REQUIREMENTS:
1. Fix every error listed in the "current_errors" array.
2. Do NOT re-introduce errors from the "previous_errors" array.
3. The output MUST conform to the Azure Standard Logic App ARM schema at API Version 2016-06-01.
4. The top-level structure MUST ONLY include EXACTLY TWO keys:
   - "definition": the corrected workflow definition
   - "parameters": the workflow parameters (can be empty {})
   Do NOT include any other top-level keys.
5. Do NOT include markdown code fences, explanations, or any text outside the JSON object.
6. Return the COMPLETE corrected workflow JSON — not just the changed parts.

RESPOND WITH JSON ONLY — no preamble, no postamble, no markdown.
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
