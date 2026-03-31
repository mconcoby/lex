from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RoleContract:
    role: str
    allowed_verbs: tuple[str, ...]
    blocked_verbs: tuple[str, ...]
    required_first_actions: tuple[str, ...]
    workflow_template: tuple[str, ...]
    system_prompt: str


ROLE_CONTRACTS: dict[str, RoleContract] = {
    "pm": RoleContract(
        role="pm",
        allowed_verbs=("task_create", "task_delegate", "task_handoff", "task_priority", "msg_send", "watch_add"),
        blocked_verbs=("task_claim",),
        required_first_actions=("review_inbox", "inspect_open_child_tasks", "assign_or_delegate_work"),
        workflow_template=(
            "Review inbox and active parent tasks.",
            "Inspect open child tasks and blockers.",
            "Delegate or reprioritize work.",
            "Request review and sequence follow-ups.",
        ),
        system_prompt=(
            "You are the PM operating through Lex. Default to delegation, sequencing, messaging, and review routing. "
            "Do not drift into direct implementation unless a human explicitly authorizes a role override."
        ),
    ),
    "dev": RoleContract(
        role="dev",
        allowed_verbs=("task_claim", "task_status", "task_handoff", "msg_send", "watch_add"),
        blocked_verbs=(),
        required_first_actions=("review_inbox", "inspect_assigned_tasks", "report_execution_plan"),
        workflow_template=(
            "Review inbox and assigned tasks.",
            "Inspect claimed task details and dependencies.",
            "Implement the requested change.",
            "Report progress, handoffs, and artifacts back into Lex.",
        ),
        system_prompt=(
            "You are a development worker operating through Lex. Default to implementation, progress reporting, and disciplined handoff. "
            "Do not assume PM authority over sequencing or assignment without an explicit override."
        ),
    ),
    "auditor": RoleContract(
        role="auditor",
        allowed_verbs=("task_create", "task_status", "msg_send", "watch_add"),
        blocked_verbs=("task_claim",),
        required_first_actions=("review_inbox", "inspect_review_queue", "record_review_plan"),
        workflow_template=(
            "Review inbox and subscribed review queues.",
            "Inspect completed work and recent decisions.",
            "Publish review outcomes or follow-up tasks.",
            "Escalate regressions and unresolved issues.",
        ),
        system_prompt=(
            "You are the auditor operating through Lex. Default to inspection, verification, review results, and follow-up tickets. "
            "Avoid taking over implementation unless explicitly documented."
        ),
    ),
    "infra": RoleContract(
        role="infra",
        allowed_verbs=("task_create", "task_claim", "task_handoff", "task_priority", "task_status", "msg_send", "watch_add"),
        blocked_verbs=(),
        required_first_actions=("review_inbox", "inspect_integration_queue", "record_integration_plan"),
        workflow_template=(
            "Review inbox and integration queue.",
            "Inspect branch, merge, and release dependencies.",
            "Coordinate integration and release tasks.",
            "Report integration outcomes and next blockers.",
        ),
        system_prompt=(
            "You are the infra coordinator operating through Lex. Default to integration, branch coordination, release handling, and controlled handoff."
        ),
    ),
}


ROLE_ACTION_LABELS = {
    "review_inbox": "Review inbox",
    "inspect_open_child_tasks": "Inspect open child tasks",
    "assign_or_delegate_work": "Assign or delegate work",
    "inspect_assigned_tasks": "Inspect assigned tasks",
    "report_execution_plan": "Report execution plan",
    "inspect_review_queue": "Inspect review queue",
    "record_review_plan": "Record review plan",
    "inspect_integration_queue": "Inspect integration queue",
    "record_integration_plan": "Record integration plan",
}


def get_role_contract(role: str | None) -> RoleContract | None:
    if not role:
        return None
    return ROLE_CONTRACTS.get(role)
