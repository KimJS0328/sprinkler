from __future__ import annotations

from typing import Any

from sprinkler import task


class Task:
    def __init__(
        self,
        id_: str,
        *,
        input_config: dict[str, Any | dict[str, Any]] | None = None,
        output_config: type | None = None,
    ) -> None:
        self.id = id_
        self.input_config = input_config
        self.output_config = output_config

    def __call__(self, func):
        return task.Task(
            self.id,
            func,
            input_config=self.input_config,
            output_config=self.output_config
        )
