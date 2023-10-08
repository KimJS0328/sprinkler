from __future__ import annotations

from typing import Any
import copy

from sprinkler.task import Task
from sprinkler.context import Context
from sprinkler import config


class Pipeline:
    """The pipeline which flows with tasks

    Attributes:
        tasks: the list of tasks
        task_id_set: the set which contains task_id of tasks
        context: the global context values for pipeline instance
    """

    def __init__(self, id_: str, context: dict[str, Any] | None = None) -> None:
        """Initializes the pipeline instance with context

        Args:
            context: 
        """
        self.id = id_
        self.tasks: list[Task] = []
        self.tasks_id_set: set = set()
        self.context: Context = Context()
        
        if context:
            self.context.add_global(context)
        

    def add_task(self, task: Task) -> None:
        """Add the task instance to pipeline

        Args:
            task: the task instance
        """
        if not isinstance(task, Task):
            raise TypeError('Given task parameter is not Task instance')
        
        if task.id in self.tasks_id_set:
            raise Exception(f'Task ID \'{task.id}\' is already exsists')
        
        self.tasks.append(task)
        self.tasks_id_set.add(task.id)
    

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

        if self.tasks:
            kwargs = self._bind_first_task_arguments(
                self.tasks[0], context_for_run, args, kwargs
            )
            output = self.tasks[0].run(**kwargs)
            context_for_run.add_history(output, self.tasks[0].id)

        # run tasks as chain with context
        for task in self.tasks[1:]:
            kwargs = self._bind_task_arguments(task, context_for_run, output)
            output = task.run(**kwargs)
            context_for_run.add_history(output, task.id)

        return output
    

    def _bind_first_task_arguments(
        self,
        task: Task,
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
        task: Task,
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

