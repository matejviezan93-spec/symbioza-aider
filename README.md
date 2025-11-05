# Symbioza Aider Agent (v1)

The Symbioza Aider agent listens to the Redis stream `symbioza:msg:symbioza-aider`,
executes incoming `ExecCommand` instructions with [aider](https://aider.chat/),
and publishes structured results back to the Symbioza messaging fabric.

## Quickstart

```bash
. .venv/bin/activate
python -m modules.symbioza_aider.runner --debug
```

## Behaviour

- Consumes backlog and live messages from `symbioza:msg:symbioza-aider`.
- Executes aider with `--yes --commit` and tries to push the resulting commit.
- Emits execution summaries to `symbioza:msg:owner` (or the requesting module).
- Appends telemetry to `logs/agent_traces.jsonl`.

## Requirements

Install the module extras when needed:

```bash
pip install -r modules/symbioza-aider/requirements.txt
```

Ensure Redis and aider are accessible from the runtime environment.
