# Codex Working Agreement

This document records the default collaboration setup for Codex in this repository.

## Default Workspace

- Repository root: `/home/wnlab/langtaosha/Langtaosha_PD`
- Default Conda environment: `langtaosha`
- Default working directory for commands: repository root

## Working Rules

- Run repository commands from `/home/wnlab/langtaosha/Langtaosha_PD` unless the user explicitly says otherwise.
- Prefer the `langtaosha` Conda environment for Python, `pytest`, and repository scripts.
- Check `git status` before making code changes when starting a new task.
- Preserve existing uncommitted work and continue on top of it unless the user asks for a different approach.
- Do not switch branches unless the user explicitly asks.
- Do not run destructive Git commands such as `git reset --hard` or `git checkout --` unless the user explicitly asks.
- Do not delete files in bulk without confirmation.

## Collaboration Notes

- If the workspace already contains user changes, treat them as intentional and avoid overwriting them.
- If a task requires a risky or potentially destructive action, pause and confirm with the user first.
- If a future session starts without this context, reuse this document as the repository-level working agreement.
