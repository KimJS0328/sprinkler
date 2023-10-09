from __future__ import annotations

from typing import Any
import copy

from sprinkler.runnable.base import Runnable
from sprinkler.context import Context
from sprinkler import config


class Pipeline:
    """The pipeline which executes `Runnable` serially

    Attributes:
        id: identifer of `Runnable`.
        members: the list of `Runnable`.
        member_id_set: the set which contains id of `Runnable`
        context: the global context values for pipeline instance
    """

    id: str
    members: list[Runnable]
    member_id_set: set
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
            raise Exception(f'Task ID \'{runnable.id}\' is already exsists')
        
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
            kwargs = self._bind_first_task_arguments(
                self.members[0], context_for_run, args, kwargs
            )
            output = self.members[0].run(**kwargs)
            context_for_run.add_history(output, self.members[0].id)

        # run tasks as chain with context
        for task in self.members[1:]:
            kwargs = self._bind_task_arguments(task, context_for_run, output)
            output = task.run(**kwargs)
            context_for_run.add_history(output, task.id)

        return output
    

    def _bind_first_task_arguments(
        self,
        task: Runnable,
        context: Context,
        args: tuple,
        kwargs: dict
    ) -> dict[str, Any]:
        
        if config.OUTPUT_KEY in kwargs:
            return self._bind_task_arguments(
                task, context, kwargs[config.OUTPUT_KEY]
            )

        query = task.get_query()
        kwargs.update(context.get_kwargs(query))

        remaining_params = [param for param, _ in query if param not in kwargs]

        if not remaining_params:
            return kwargs

        for param, val in zip(remaining_params, args):
            kwargs[param] = val

        return kwargs
         

    def _bind_task_arguments(
        self,
        task: Runnable,
        context: Context,
        previous_output: Any
    ) -> dict[str, Any]:

        query = task.get_query()
        kwargs = context.get_kwargs(query)
        remaining_params = [param for param, _ in query if param not in kwargs]

        if not remaining_params:
            return kwargs

        if len(remaining_params) == 1:
            kwargs[remaining_params[0]] = previous_output
        
        elif all(
            hasattr(previous_output, attr) 
            for attr in ('keys', '__getitem__', '__contains__')
        ):
            for param in remaining_params:
                if param in previous_output:
                    kwargs[param] = previous_output[param]

        elif all(
            hasattr(previous_output, attr) 
            for attr in ('__iter__', '__len__')
        ):
            for param, val in zip(remaining_params, previous_output):
                kwargs[param] = val

        
        return kwargs

