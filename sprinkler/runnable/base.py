from __future__ import annotations

from typing import Any

from sprinkler.context.base import Context


class Runnable:
    def run(self, *args, **kwargs) -> Any:
        """run the task with given context."""
        raise NotImplementedError


    def run_with_context(
        self,
        context_: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        raise NotImplementedError