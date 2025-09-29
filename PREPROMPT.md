# Ollama AWS CLI Helper

## Goal
Convert natural language requests such as "list my s3 buckets" into concrete AWS CLI commands and optionally execute them.

## Pre-instructions for AI agents
- Always respond with a single AWS CLI command beginning with `aws`.
- Do not emit explanations, code fences, placeholders, or commentary unless the user explicitly asks for help.
- Prefer safe query defaults (e.g., use `aws s3 ls` for bucket listings).
- If the request cannot be satisfied with a single AWS CLI command, state that explicitly instead of guessing.

## Usage flow
1. Ensure the chosen LLM backend is available:
   - For Ollama, pull the required model (default `llama3.1`).
   - For the Claude CLI, install it, set `CLAUDE_API_KEY`, and login if necessary.
2. Verify the AWS CLI is installed and credentials are configured (`aws configure`).
3. Run the helper with the desired provider (Ollama is default):
   ```bash
   python3 ollama_aws_cli.py "list my s3 buckets"
   python3 ollama_aws_cli.py --provider claude "list my s3 buckets"
   ```
4. Review the generated command printed to the terminal. Use `--dry-run` to skip execution while testing prompts.
5. If a provider is unavailable (for example, Ollama service down or Claude login missing), the helper falls back to a simple heuristic (for example, mapping "list my s3 buckets" to `aws s3 ls`).
6. Adjust `ollama_aws_config.json` to change the provider defaults, model selections, or prompt templates if the outputs need tuning.

## Sample prompt tuning
If the model adds extra wording, tighten the `system_prompt` in `ollama_aws_config.json` to reiterate "output the command only".
