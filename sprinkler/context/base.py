from __future__ import annotations

from typing import Any

from sprinkler import config


class Context:
    """Context for task and pipline(process)
    
    Attributes:
        local_context: context values for current task
        output_context: context values for output of previous tasks
        global_context: context values for being used in entire process
        history_context: context values for recording context for specific task (monitoring)
    """
    
    def __init__(self) -> None:
        """Initializes all contexts to empty dictionary"""
        self.local_context = {}
        self.output_context = {} 
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
    

    def set_local_context(self, context: dict[str, Any]) -> None:
        """Set local context (before task begins)

        Args:
            context: dictionary with {source}: {value} 
        """
        self.local_context.update(context)

    def clear_local_context(self) -> None:
        """Remove all values in local context (after task finished)"""
        self.local_context.clear()
    
    def add_global_context(self, context: dict[str, Any]) -> None:
        """Add some values to global context
        
        Args:
            context: dictionary with {source}: {value}
        """
        self.global_context.update(context)

    def add_history_context(self, output: dict[str, Any], task_id: str) -> None:
        """Record specific tasks's output to history context
        
        Args:
            output: output of tasks. dictionary with {source}: {value} 
            task_id: identifier of speicific task
        """
        if task_id in self.history_context:
            raise Exception(f'{task_id} is already recorded in history context.')
        self.history_context.update({task_id: output})

    def replace_output_context(self, output: dict[str, Any]) -> None:
        """Replace with tasks's output
        
        gurantees that previous output values is removed

        Args:
            context: dictionary with {source}: {value} 
        """
        self.output_context.clear()
        self.output_context.update(output)

    def get_output(self) -> Any:
        """Retrive output of current context
        
        """
        if config.DEFAULT_OUTPUT_KEY in self.output_context:
            return self.output_context[config.DEFAULT_OUTPUT_KEY]
        else:
            return self.output_context

    def __str__(self) -> str:
        """Get all values from context

        For debugging

        Returns:
            dictionary representation of all contexts
        """
        return (f'Local Context: {self.local_context}\n'
                + f'Output Context: {self.output_context}\n'
                + f'Global Context: {self.global_context}\n'
                + f'History Context: {self.history_context}\n')