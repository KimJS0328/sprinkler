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
        context_for_run = copy.deepcopy(self.context)
        if 'context' in kwargs: 
            context = kwargs.pop('context')
            context_for_run.add_global(context)

        # execute the first task with given arguments
        if self.tasks:
            output = self.tasks[0].execute(*args, **kwargs)
            context_for_run.replace_output(output)
            context_for_run.add_history(output, self.tasks[0].id)

        # execute tasks as chain with context
        for task in self.tasks[1:]:
            args, kwargs = context_for_run.get_args(task.get_query())
            output = task.execute(*args, **kwargs)
            context_for_run.replace_output(output)
            context_for_run.add_history(output, task.id)

        return context_for_run.get_output()