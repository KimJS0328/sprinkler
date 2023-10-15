from __future__ import annotations

from typing import Any
from collections import OrderedDict
from collections.abc import Mapping


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


    def get_kwargs(self, query: OrderedDict[str, str]) -> dict[str, Any]:
        """Retrieve arguments in context requested by query

        result doesn't include value which doesn't exist in context
        priority: history(if task_id is specifed) -> global
        
        Args:
            query: key-value pair with {argument_name}: {source}

        Returns:
            Tuple of arguments (tuple) and keyword argument (dictionary)
        """
        kwargs = {}

        for param, src in query.items():
            # match argument with history context
            result = self._recursive_search(src.split('.'), self.history_context)

            if result is not self.NotFound:
                kwargs[param] = result
            else:
                # match argument with global context
                if src in self.global_context:
                    kwargs[param] = self.global_context[src]

        return kwargs

    
    def add_global(self, context: dict[str, Any]) -> None:
        """Add some values to global context
        
        Args:
            context: dictionary with {source}: {value}
        """
        if not isinstance(context, dict):
            raise TypeError(f'Given context must be dictionary.')
        self.global_context.update(context)


    def add_history(self, output: Any, runnable_id: str) -> None:
        """Record specific tasks's output to history context
        
        Args:
            output: output of task
            task_id: identifier of task
        """
        if runnable_id in self.history_context:
            raise Exception(f'{runnable_id} is already recorded in history context.')
        self.history_context.update({runnable_id: output})


    def update(self, context: Context) -> None:
        self.global_context.update(context.global_context)
        self.history_context.update(context.history_context)


    class NotFound:
        ...


    def _recursive_search(self, path: list[str], target: Mapping) -> Any:
        for key in path:
            try:
                target = target[key]
            except:
                return self.NotFound

        return target


    def __str__(self) -> str:
        """Get all values from context

        For monitoring of current state

        Returns:
            dictionary representation of all contexts
        """
        return (f'Global Context: {self.global_context}\n'
                + f'History Context: {self.history_context}\n'
                + f'Output: {self.output}\n')
