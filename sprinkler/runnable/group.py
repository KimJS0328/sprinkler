from __future__ import annotations

from typing import Any
import copy

from sprinkler.runnable.base import Runnable
from sprinkler.context.base import Context
from sprinkler import config


class Group(Runnable):
    """The group of parallel running `Runnable`"""

    id: str
    members: list[Runnable]
    member_id_set: set[str]
    context: Context


    def __init__(self, id_: str, context: dict[str, Any] = None) -> None:
        """Initialize the Group with context.
        
        Args:
            context: 
        """
        self.id = id_
        self.members = []
        self.member_id_set = set()
        self.context = Context()

        if context:
            self.context.add_global(context)


    def add(self, runnable: Runnable):
        """Add new `Runnable` instance to this group"""
        if not isinstance(runnable, Runnable):
            raise TypeError('Given task parameter is not `Runnable` instance')
        
        if runnable.id in self.member_id_set:
            raise Exception(f'`Runnable` \'{runnable.id}\' is already exsists')
        
        self.members.append(runnable)
        self.member_id_set.add(runnable.id)


    def run(self, **inputs) -> dict[str, Any]:
        """
        """
        return self.run_with_context({}, **inputs)


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
        
        for runnable in self.members:
            input_ = (
                inputs.get(runnable.id) 
                or inputs.get(config.DEFAULT_GROUP_INPUT_KEY) 
                or None
            )

            results[runnable.id] = runnable.run_with_context(
                copy.deepcopy(context_for_run),
                **{config.OUTPUT_KEY: input_}
            )
        
        return results