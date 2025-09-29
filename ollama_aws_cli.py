#!/usr/bin/env python3
"""Translate natural language AWS requests into AWS CLI commands via Ollama."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

DEFAULT_CONFIG_PATH = Path(__file__).with_name("ollama_aws_config.json")
DEFAULT_PROVIDER = "ollama"
DEFAULT_SYSTEM_PROMPT = (
    "You translate user intents into exact AWS CLI commands. "
    "Reply with a single line containing only the command beginning with 'aws'. "
    "Do not include explanations, placeholders, shell prompts, or Markdown fences."
)
DEFAULT_USER_PROMPT_TEMPLATE = (
    "Convert the following request into an AWS CLI command: {query}"
)
DEFAULT_PROVIDER_MODELS = {
    "ollama": "llama3.1",
    "claude": "claude-3-5-sonnet-latest",
}


def load_config(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse config file {path}: {exc}") from exc


def call_ollama(
    model: str,
    system_prompt: str,
    user_prompt: str,
    provider_config: Dict[str, Any],
) -> str:
    def _invoke(cmd: list[str]) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env_override,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "The 'ollama' executable was not found. Install Ollama or adjust PATH."
            ) from exc

    binary = provider_config.get("binary", "ollama")
    run_args = provider_config.get("run_args", [])
    if not isinstance(run_args, list):
        raise RuntimeError("'run_args' must be a list of strings in provider config")

    env_override = None
    if "env" in provider_config:
        env_override = os.environ.copy()
        env_override.update({k: str(v) for k, v in provider_config["env"].items()})

    primary_cmd = [binary, "run", model, *run_args]
    if system_prompt:
        primary_cmd.extend(["--system", system_prompt])
    primary_cmd.append(user_prompt)

    result = _invoke(primary_cmd)
    if result.returncode == 0:
        return result.stdout

    stderr = result.stderr.strip()
    exit_code = result.returncode
    unknown_flag = "unknown flag" in stderr.lower() and "--system" in stderr
    if system_prompt and unknown_flag:
        merged_prompt = f"{system_prompt.strip()}\n\n{user_prompt.strip()}"
        fallback_cmd = [binary, "run", model, *run_args, merged_prompt]
        fallback_result = _invoke(fallback_cmd)
        if fallback_result.returncode == 0:
            return fallback_result.stdout
        stderr = fallback_result.stderr.strip()
        exit_code = fallback_result.returncode

    raise RuntimeError(
        f"Ollama invocation failed with exit code {exit_code}:\n{stderr}"
    )


def call_claude(
    model: str,
    system_prompt: str,
    user_prompt: str,
    provider_config: Dict[str, Any],
) -> str:
    try:
        import anthropic
    except ImportError as exc:
        raise RuntimeError(
            "The 'anthropic' package is required for Claude API calls. Install it with: pip install anthropic"
        ) from exc

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Also check common alternative environment variable names
        api_key = os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY (or CLAUDE_API_KEY) environment variable is required for Claude API calls. "
            "Please set it with your Anthropic API key."
        )

    client = anthropic.Anthropic(api_key=api_key)
    
    messages = []
    if user_prompt:
        messages.append({"role": "user", "content": user_prompt.strip()})

    try:
        response = client.messages.create(
            model=model,
            max_tokens=1000,
            system=system_prompt.strip() if system_prompt else None,
            messages=messages
        )
        
        return response.content[0].text if response.content else ""
        
    except Exception as exc:
        raise RuntimeError(f"Claude API call failed: {exc}") from exc


def call_backend(
    provider: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    provider_config: Dict[str, Any],
) -> str:
    normalized = provider.lower()
    if normalized == "ollama":
        return call_ollama(model, system_prompt, user_prompt, provider_config)
    if normalized == "claude":
        return call_claude(model, system_prompt, user_prompt, provider_config)
    raise RuntimeError(f"Unsupported provider '{provider}'.")


def extract_aws_command(raw_response: str) -> str:
    for line in raw_response.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("```"):
            # Skip Markdown fences if the model includes them despite instructions.
            continue
        if stripped.lower().startswith("command:"):
            stripped = stripped.partition(":")[2].strip()
        if stripped.startswith("aws "):
            return stripped
    raise RuntimeError(
        "Could not find an AWS CLI command in the Ollama response. Response was:\n"
        f"{raw_response.strip()}"
    )


def run_aws_cli(command: str, dry_run: bool) -> int:
    tokens = shlex.split(command)
    if not tokens or tokens[0] != "aws":
        raise RuntimeError(f"Refusing to run non-AWS command: {command}")

    print(f"\n> {command}")
    if dry_run:
        print("(dry-run) Skipping execution.")
        return 0

    try:
        completed = subprocess.run(tokens, check=True)
    except FileNotFoundError as exc:
        raise RuntimeError(
            "The 'aws' executable was not found. Install the AWS CLI and ensure it is on PATH."
        ) from exc
    except subprocess.CalledProcessError as exc:
        return exc.returncode

    return completed.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an AWS CLI command via Ollama from a natural language request and execute it."
        )
    )
    parser.add_argument("query", help="Natural language description, e.g. 'list my S3 buckets'.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config file (default: {DEFAULT_CONFIG_PATH.name}).",
    )
    parser.add_argument(
        "--provider",
        help="Model provider backend to use (e.g. 'ollama' or 'claude').",
    )
    parser.add_argument(
        "--model",
        help="Override the model name defined for the chosen provider.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate the AWS CLI command but do not execute it.",
    )
    return parser


def resolve_provider_settings(
    config: Dict[str, Any],
    provider: str,
) -> Tuple[Dict[str, Any], str, str, str, str]:
    normalized_provider = provider.strip().lower() if provider else DEFAULT_PROVIDER
    providers_section = config.get("providers")

    provider_config: Dict[str, Any] = {}
    if isinstance(providers_section, dict):
        normalized_map = {key.lower(): value for key, value in providers_section.items()}
        provider_config = normalized_map.get(normalized_provider)
        if provider_config is None:
            available = ", ".join(sorted(normalized_map.keys())) or "(none)"
            raise RuntimeError(
                f"Provider '{provider}' not defined in configuration. Available providers: {available}"
            )
        provider_config = dict(provider_config)
    else:
        provider_config = dict(config)

    system_prompt = provider_config.get("system_prompt") or config.get("system_prompt") or DEFAULT_SYSTEM_PROMPT
    user_prompt_template = (
        provider_config.get("user_prompt_template")
        or config.get("user_prompt_template")
        or DEFAULT_USER_PROMPT_TEMPLATE
    )

    if provider_config.get("model") is not None:
        model = str(provider_config["model"])
    elif isinstance(providers_section, dict):
        model = DEFAULT_PROVIDER_MODELS.get(
            normalized_provider, DEFAULT_PROVIDER_MODELS[DEFAULT_PROVIDER]
        )
    else:
        model = str(
            config.get(
                "model",
                DEFAULT_PROVIDER_MODELS.get(
                    normalized_provider, DEFAULT_PROVIDER_MODELS[DEFAULT_PROVIDER]
                ),
            )
        )

    display_name = provider_config.get("display_name") or normalized_provider.title()
    return provider_config, system_prompt, user_prompt_template, model, display_name


def fallback_command(query: str) -> Optional[str]:
    normalized = query.lower()
    if "s3" in normalized and "bucket" in normalized:
        if any(keyword in normalized for keyword in ("list", "show", "display", "enumerate")):
            return "aws s3 ls"
    return None


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)

    requested_provider = args.provider or config.get("default_provider") or DEFAULT_PROVIDER
    provider = requested_provider.strip().lower()

    try:
        (
            provider_config,
            system_prompt,
            user_prompt_template,
            model_from_config,
            display_name,
        ) = resolve_provider_settings(config, provider)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    model = args.model or model_from_config
    user_prompt = user_prompt_template.format(query=args.query.strip())

    print(f"Using {display_name} model '{model}' to interpret request...")
    warning_lines: list[str] = []

    try:
        raw_response = call_backend(provider, model, system_prompt, user_prompt, provider_config)
    except RuntimeError as exc:
        fallback = fallback_command(args.query)
        if fallback:
            warning_lines.append(
                f"Warning: {display_name} call failed; using heuristic fallback command."
            )
            warning_lines.append(f"Reason: {exc}")
            command = fallback
        else:
            print(str(exc), file=sys.stderr)
            return 1
    else:
        command = extract_aws_command(raw_response)

    for line in warning_lines:
        print(line)

    try:
        return run_aws_cli(command, args.dry_run)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
