from __future__ import annotations

from typing import Any
import copy

from sprinkler.pipeline.base import Pipeline
from sprinkler.context import Context


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


    def run(self, context: dict[str, Any] | Context = None) -> dict[str, Any]:
        context_for_run = copy.deepcopy(self.context)

        if isinstance(context, Context):
            context_for_run.update(context)
        elif isinstance(context, dict):
            context_for_run.add_global(context)

        results = {}
        
        for pipeline in self.members:
            results[pipeline.id] = pipeline.run(copy.deepcopy(context_for_run))
        
        return results