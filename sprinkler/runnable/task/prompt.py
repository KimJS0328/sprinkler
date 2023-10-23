from __future__ import annotations

from typing import Any, List, Dict

from sprinkler.runnable.task import Task
from sprinkler.operation import construct_messages


class PromptTask(Task):
    """Task Class for constructing templates"""
    def __init__(
        self, 
        id_: str,
        context_: Dict[str | Any] | None = None,
    ) -> None:

        super().__init__(id_, 
                        construct_messages,
                        context=context_)