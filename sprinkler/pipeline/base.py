from __future__ import annotations

from typing import Any
import copy

from sprinkler.task import Task
from sprinkler.context import Context


class Pipeline:
    """The pipeline which flows with tasks

    Attributes:
        tasks: the list of tasks
        task_id_set: the set which contains task_id of tasks
        context: the global context values for pipeline instance
    """

    id: str
    tasks: list[Task]
    tasks_id_set: set
    context: Context

    def __init__(self, id_: str, context: dict[str, Any] = None) -> None:
        """Initializes the pipeline instance with context

        Args:
            context: 
        """
        self.id = id_
        self.tasks = []
        self.tasks_id_set = set()
        self.context = Context()
        
        if context:
            self.context.add_global_context(context)
        

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


    def run(self, context: dict[str, Any] | Context = None) -> Any:
        """run the pipeline flows

        excute the tasks in the pipeline sequentially

        Args:
            context: global context values used to run this pipeline

        Returns:
            final output of pipeline
        """
        context_for_run = copy.deepcopy(self.context)

        if isinstance(context, Context):
            context_for_run.update(context)
        elif isinstance(context, dict):
            context_for_run.add_global_context(context)

        for task in self.tasks:
            task.execute(context_for_run)

        return context_for_run.get_output()

    
    def __call__(self, context: dict[str, Any] = None) -> Any:
        return self.run(context)
