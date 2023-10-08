from __future__ import annotations

from typing import Any
import copy

from sprinkler.pipeline.base import Pipeline
from sprinkler.context import Context
from sprinkler import config


class Group:
    """The group of parallel running pipelines"""

    id: str
    context: Context
    members: list[Pipeline]


    def __init__(self, id_: str, context: dict[str, Any] = None) -> None:
        """Initialize the Group with context.
        
        Args:
            context: 
        """
        self.id = id_
        self.context = Context()
        self.members = []

        if context:
            self.context.add_global(context)


    def add_pipeline(self, pipeline: Pipeline):
        """Add new pipeline to this group"""
        self.members.append(pipeline)


    def run(self, **inputs) -> dict[str, Any]:
        """
        """
        return self.run_with_context({}, inputs)


    def run_with_context(
        self, 
        context_: dict[str, Any] | Context,
        **inputs
    ) -> dict[str, Any]:
        
        context_for_run = copy.deepcopy(self.context)

        if isinstance(context_, dict):
            context_for_run.add_global(context_)
        elif isinstance(context_, Context):
            context_for_run.update(context_)

        results = {}
        
        for pipeline in self.members:
            input_ = (
                inputs.get(pipeline.id) 
                or inputs.get(config.DEFAULT_GROUP_INPUT_KEY) 
                or {}
            )

            results[pipeline.id] = pipeline.run_with_context(
                copy.deepcopy(context_for_run),
                input_
            )
        
        return results