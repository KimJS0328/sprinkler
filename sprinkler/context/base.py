from __future__ import annotations

from typing import Any


class Context:
    """Context for task and pipline(process)
    
    Attributes:
        output: output of previous tasks
        global_context: context values for being used in entire process
        history_context: context values for recording context for specific task (monitoring)
    """

    output: Any
    global_context: dict
    history_context: dict
    
    def __init__(self) -> None:
        """Initializes all contexts to empty dictionary"""
        self.output = None
        self.global_context = {} 
        self.history_context = {}

    def get_values(self, query: dict[str, Any]) -> dict[str, Any]:
        """Retrieve values in context requested by query

        result doesn't include value which doesn't exist in context
        priority: history(if task_id is specifed) -> local -> previous output -> global
        
        Args:
            query: key-value pair with {argument_name}: {source}

        Returns:
            A dict mapping argument name to the correspoding value in the context
            {argument_name}: {value}
        """
        result = {}

        for arg, src in query.items():
            src_parts = src.split(sep='.', maxsplit=2)
            value = None

            if src_parts == 2: 
                task_id, arg = src_parts
                if task_id in self.history_context:
                    value = self.history_context[task_id].get(arg) 
            else:
                value = (
                    self.local_context.get(src) 
                    or self.output_context.get(src) 
                    or self.global_context.get(src)
                )

            if value: result[arg] = value

        return result

    
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

    def replace_output(self, output: Any) -> None:
        """Replace with tasks's output"""
        self.output = output

    def get_output(self) -> Any:
        """Retrive output of current context"""
        return self.output

    def __str__(self) -> str:
        """Get all values from context

        For monitoring of current state

        Returns:
            dictionary representation of all contexts
        """
        return (f'Global Context: {self.global_context}\n'
                + f'History Context: {self.history_context}'
                + f'Output: {self.output}\n')