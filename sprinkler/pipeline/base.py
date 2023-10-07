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

    def __init__(self, context: dict[str, Any] = None) -> None:
        """Initializes the pipeline instance with context

        Args:
            context: 
        """
        self.tasks: list[Task] = []
        self.tasks_id_set: set = set()
        self.context: Context = Context()
        
        if context: self.context.add_global(context)
        

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
    

    def run_with_context(self, context, *args, **kwargs) -> Any:
        context_for_run = copy.deepcopy(self.context)

        # add to global context in pipeline
        if context: 
            context_for_run.add_global(context)
        
        # run the first task with given arguments
        if self.tasks:
            output = self.tasks[0].run(*args, **kwargs)
            context_for_run.add_history(output, self.tasks[0].id)

        # run tasks as chain with context
        for task in self.tasks[1:]:
            kwargs = self._bind_arguments(context_for_run, task, output)
            output = task.run(**kwargs)
            context_for_run.add_history(output, task.id)

        return output
    

    def _bind_arguments(
        self,
        context: Context,
        task: Task,
        previous_output: Any
    ) -> dict[str, Any]:
        query = task.get_query()
        kwargs = context.get_kwargs(query)

        remaining_args = [arg for arg, _ in query if arg not in kwargs]

        if not remaining_args:
            return kwargs


        if len(remaining_args) == 1:
            kwargs[remaining_args[0]] = previous_output
        
        elif (
            all(
                hasattr(previous_output, attr) 
                for attr in ('keys', '__getitem__')
            )
            and all(
                arg in previous_output
                for arg in remaining_args
            )
        ):
            for arg in remaining_args:
                kwargs[arg] = previous_output[arg]

        elif (
            all(
                hasattr(previous_output, attr) 
                for attr in ('__iter__', '__len__')
            )
            and len(previous_output) == len(remaining_args)
        ):
            for i, arg in enumerate(remaining_args):
                kwargs[arg] = previous_output[i]
        
        else:
            raise Exception((
                f'Task {task.id}: '
                f'input arguments {remaining_args} cannot find the value.'
            ))
        
        return kwargs

