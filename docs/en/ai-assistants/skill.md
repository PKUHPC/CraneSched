# CraneSched Skill

The CraneSched Skill is a set of usage and first-line troubleshooting instructions for AI assistants. It helps users:

- write and review jobs using `cbatch`, `calloc`, `crun`, and related commands;
- understand queues, job states, pending reasons, and exit codes;
- troubleshoot resource requests, GPUs, MPI, multi-node jobs, containers, and failures;
- separate user-level checks from cluster issues that require an administrator.

The Skill grants no additional permissions and does not replace site documentation or policy. Answers should follow the installed command's `--help`, site guidance, and the user's actual output.

## Who can use it

Any CraneSched user can install the Skill in a personal AI assistant. It is independent of the
[shared cluster Codex](shared-codex.md): users can use it with their own Codex, Claude Code, or
another Agent that reads `SKILL.md`, even when the cluster administrator has not deployed Codex.

## Install

The canonical source is
[`docs/skills/cranesched-skill`](https://github.com/PKUHPC/CraneSched/tree/master/docs/skills/cranesched-skill).
Install the complete directory rather than copying only `SKILL.md`; the `references/` directory is
part of the Skill.

This example uses a sparse clone and installs the Skill in the common Agent Skills directory:

```bash
skill_source="${XDG_DATA_HOME:-$HOME/.local/share}/cranesched-skill-source"
git clone --depth 1 --filter=blob:none --sparse \
  https://github.com/PKUHPC/CraneSched.git "${skill_source}"
git -C "${skill_source}" sparse-checkout set docs/skills/cranesched-skill
mkdir -p "${HOME}/.agents/skills"
ln -s "${skill_source}/docs/skills/cranesched-skill" \
  "${HOME}/.agents/skills/cranesched-skill"
```

- **Codex and Agent Skills-compatible tools:** use `~/.agents/skills/`, or the tool's documented user-level Skill directory.
- **Claude Code:** change `~/.agents/skills` in the last two commands to `~/.claude/skills`.
- **Agents without automatic Skill discovery:** keep the source directory and explicitly ask the Agent to read its `SKILL.md` before handling a CraneSched question.

Restart the Agent or open a new session, then ask it to use `cranesched-skill`, or simply request a
CraneSched task such as "write a CraneSched GPU job script." Update the Skill with:

```bash
git -C "${skill_source}" pull --ff-only
```
