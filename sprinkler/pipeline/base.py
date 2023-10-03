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
        
        if task.task_id in self.tasks_id_set:
            raise Exception(f'Task ID \'{task.task_id}\' is already exsists')

        self.tasks.append(task)
        self.tasks_id_set.add(task.task_id)

    def run_sequential(self, context: dict[str, Any] = None) -> Any:
        """run the pipeline flows

        excute the tasks in the pipeline sequentially

        Args:
            context: global context values used to run this pipeline

        Returns:
            final output of pipeline
        """
        context_for_run = copy.deepcopy(self.context)
        if context: context_for_run.add_global(context)

        for task in self.tasks:
            output = task.execute(context_for_run)
            context_for_run.add_history(output, task.task_id)

        return context_for_run.get_output()