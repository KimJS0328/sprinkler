from __future__ import annotations

from typing import Any
import copy

from sprinkler.runnable.base import Runnable
from sprinkler.context import Context
from sprinkler import config


class Pipeline(Runnable):
    """The pipeline which executes `Runnable` serially

    Attributes:
        id: identifer of `Runnable`.
        members: the list of `Runnable`.
        member_id_set: the set which contains id of `Runnable`
        context: the global context values for pipeline instance
    """

    id: str
    members: list[Runnable]
    member_id_set: set[str]
    context: Context

    def __init__(self, id_: str, context: dict[str, Any] | None = None) -> None:
        """Initializes the pipeline instance with context

        Args:
            context: 
        """
        self.id = id_
        self.members= []
        self.member_id_set = set()
        self.context = Context()
        
        if context:
            self.context.add_global(context)
        

    def add(self, runnable: Runnable) -> None:
        """Add the `Runnable` instance to pipeline

        Args:
            runnable: the Runnable instance
        """
        if not isinstance(runnable, Runnable):
            raise TypeError('Given task parameter is not `Runnable` instance')
        
        if runnable.id in self.member_id_set:
            raise Exception(f'`Runnable` \'{runnable.id}\' is already exsists')
        
        self.members.append(runnable)
        self.member_id_set.add(runnable.id)
    

    def run(self, *args, **kwargs) -> Any:
        """run the pipeline flows

        excute the tasks in the pipeline sequentially

        Returns:
            final output of pipeline
        """
        return self.run_with_context({}, *args, **kwargs)
    

    def run_with_context(self, context_: dict[str, Any], *args, **kwargs) -> Any:
        context_for_run = copy.deepcopy(self.context)

        # add to global context in pipeline
        if isinstance(context_, dict):
            context_for_run.add_global(context_)
        elif isinstance(context_, Context):
            context_for_run.update(context_)
        
        if self.members:
            runnable = self.members[0]
            output = runnable.run_with_context(context_for_run, *args, **kwargs)
            context_for_run.add_history(output, runnable.id)

        # run tasks as chain with context
        for runnable in self.members[1:]:
            output = runnable.run_with_context(
                context_for_run, 
                **{config.OUTPUT_KEY: output}
            )
            context_for_run.add_history(output, runnable.id)

        return output


    async def arun(self, *args, **kwargs) -> Any:
        return self.arun_with_context({}, *args, **kwargs)


    async def arun_with_context(
        self,
        context_: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        return self.run_with_context(context_, *args, **kwargs)


    def __call__(self, *args, **kwargs) -> Any:
        return self.run(*args, **kwargs)