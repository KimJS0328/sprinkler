from __future__ import annotations

from typing import Any, Dict

from sprinkler.runnable.task import Task
from sprinkler.operations import construct_messages, aconstruct_messages


class PromptTask(Task):
    """Task Class for constructing templates"""
    def __init__(
        self, 
        id_: str,
        *,
        context: Dict[str | Any] | None = None,
        async_mode: bool = False
    ) -> None:

        if not async_mode:
            super().__init__(id_, 
                            construct_messages,
                            context=context)
        else:
            super().__init__(id_, 
                            aconstruct_messages,
                            context=context) 