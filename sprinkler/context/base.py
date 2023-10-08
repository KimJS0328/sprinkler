from __future__ import annotations

from typing import Any


class Context:
    """Context for task and pipline(process)
    
    Attributes:
        output: output of previous tasks
        global_context: context values for being used in entire process
        history_context: context values for recording context for specific task (monitoring)
    """

    global_context: dict
    history_context: dict
    
    def __init__(self) -> None:
        """Initializes all contexts to empty dictionary"""
        self.global_context = {} 
        self.history_context = {}


    def get_kwargs(self, query: list[tuple[str, str]]) -> dict[str, Any]:
        """Retrieve arguments in context requested by query

        result doesn't include value which doesn't exist in context
        priority: history(if task_id is specifed) -> global
        
        Args:
            query: key-value pair with {argument_name}: {source}

        Returns:
            Tuple of arguments (tuple) and keyword argument (dictionary)
        """
        kwargs = {}

        for arg, src in query:
            # match argument with history context
            if src in self.history_context:
                kwargs[arg] = self.history_context[src]
            else:
                # match argument with global context
                if src in self.global_context:
                    kwargs[arg] = self.global_context[src]

        return kwargs

    
    def add_global(self, context: dict[str, Any]) -> None:
        """Add some values to global context
        
        Args:
            context: dictionary with {source}: {value}
        """
        if not isinstance(context, dict):
            raise TypeError(f'Given context must be dictionary.')
        self.global_context.update(context)


    def add_history(self, output: Any, task_id: str) -> None:
        """Record specific tasks's output to history context
        
        Args:
            output: output of task
            task_id: identifier of task
        """
        if task_id in self.history_context:
            raise Exception(f'{task_id} is already recorded in history context.')
        self.history_context.update({task_id: output})


    def __str__(self) -> str:
        """Get all values from context

        For monitoring of current state

        Returns:
            dictionary representation of all contexts
        """
        return (f'Global Context: {self.global_context}\n'
                + f'History Context: {self.history_context}\n'
                + f'Output: {self.output}\n')