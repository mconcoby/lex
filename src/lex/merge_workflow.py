from __future__ import annotations

import difflib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MergePaths:
    root: Path
    rex_runtime: Path
    context_dir: Path
    proposal_dir: Path
    plan_path: Path


def resolve_merge_paths(root: Path) -> MergePaths:
    rex_runtime = root / ".lex" / "runtime"
    return MergePaths(
        root=root,
        rex_runtime=rex_runtime,
        context_dir=rex_runtime / "install-merge-context",
        proposal_dir=rex_runtime / "install-merge-proposal",
        plan_path=rex_runtime / "install-merge-plan.md",
    )


def ensure_merge_dirs(paths: MergePaths) -> None:
    paths.context_dir.mkdir(parents=True, exist_ok=True)
    paths.proposal_dir.mkdir(parents=True, exist_ok=True)


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_merge_plan(agent_kind: str) -> str:
    return f"""# Assisted Merge Plan

Preferred merge assistant: `{agent_kind}`

## Objective

Integrate `lex` into existing root agent files without erasing project-specific behavior.

## Hard Constraints

- Preserve any existing router system unless it is clearly redundant
- Preserve explicit role splits such as PM, Dev, reviewer, or routing agents
- Add `lex` as a coordination substrate, not a replacement for project-specific instructions
- Prefer small integration sections over full rewrites
- If conflicts are structural, propose options instead of forcing a merge

## Inputs

- Existing root agent files copied into `.lex/runtime/install-merge-context/`
- Canonical lex bridge references copied into the same context directory

## Expected Proposal

Write proposed merged root files to:

- `.lex/runtime/install-merge-proposal/AGENTS.md`
- `.lex/runtime/install-merge-proposal/CLAUDE.md`

If one file does not need changes, still write a proposal file that explains why the original should remain effectively unchanged.
"""


def build_bridge_reference(agent_kind: str) -> str:
    if agent_kind == "codex":
        return "Read `.lex/adapters/codex/AGENTS.md` before starting work.\n"
    if agent_kind == "gemini":
        return "Read `.lex/adapters/gemini/GEMINI.md` before starting work.\n"
    return "Read `.lex/adapters/claude/CLAUDE.md` before starting work.\n"


def create_merge_packet(root: Path, *, agent_kind: str) -> MergePaths:
    paths = resolve_merge_paths(root)
    ensure_merge_dirs(paths)

    write_text(paths.plan_path, build_merge_plan(agent_kind))

    agents_root = root / "AGENTS.md"
    claude_root = root / "CLAUDE.md"
    write_text(paths.context_dir / "AGENTS.original.md", read_text(agents_root))
    write_text(paths.context_dir / "CLAUDE.original.md", read_text(claude_root))
    write_text(paths.context_dir / "lex-codex-bridge.md", build_bridge_reference("codex"))
    write_text(paths.context_dir / "lex-claude-bridge.md", build_bridge_reference("claude"))
    write_text(paths.context_dir / "lex-gemini-bridge.md", build_bridge_reference("gemini"))

    if not (paths.proposal_dir / "AGENTS.md").exists():
        write_text(
            paths.proposal_dir / "AGENTS.md",
            "<!-- write assisted merge proposal for AGENTS.md here -->\n",
        )
    if not (paths.proposal_dir / "CLAUDE.md").exists():
        write_text(
            paths.proposal_dir / "CLAUDE.md",
            "<!-- write assisted merge proposal for CLAUDE.md here -->\n",
        )

    return paths


def unified_diff(original_path: Path, proposal_path: Path, label: str) -> str:
    original = read_text(original_path).splitlines(keepends=True)
    proposed = read_text(proposal_path).splitlines(keepends=True)
    if not proposed:
        return ""
    return "".join(
        difflib.unified_diff(
            original,
            proposed,
            fromfile=f"{label} (current)",
            tofile=f"{label} (proposal)",
        )
    )


def apply_proposal(root: Path) -> list[str]:
    paths = resolve_merge_paths(root)
    applied: list[str] = []
    for filename in ("AGENTS.md", "CLAUDE.md"):
        proposal = paths.proposal_dir / filename
        if proposal.exists() and read_text(proposal).strip():
            write_text(root / filename, read_text(proposal))
            applied.append(filename)
    return applied
