from __future__ import annotations

from enum import Enum

from typing import Dict, Tuple, Any


class Context:
    """Context for task and pipline(process)
    
    Attributes:
        output: output of previous tasks
        global_context: context values for being used in entire process
        history_context: context values for recording context for specific task (monitoring)
    """

    output: Any
    global_context: Dict
    history_context: Dict
    
    def __init__(self) -> None:
        """Initializes all contexts to empty dictionary"""
        self.output = None
        self.global_context = {} 
        self.history_context = {}

    def get_args(self, query: dict[str, Any]) -> Tuple[Tuple, Dict[str, Any]]:
        """Retrieve arguments in context requested by query

        result doesn't include value which doesn't exist in context
        priority: history(if task_id is specifed) -> output -> global
        
        Args:
            query: key-value pair with {argument_name}: {source}

        Returns:
            Tuple of arguments (tuple) and keyword argument (dictionary)
        """
        args = ()
        kwargs = {}

        # validation of output type
        output_mapping = False # flag for output mapping to kwargs

        if self.output is None:
            pass
        elif (all(hasattr(self.output, attr) # output is a mapping object
                for attr in ('keys', '__getitem__'))):
            output_mapping = True
        elif (all(hasattr(self.output, attr)
                for attr in ('__iter__', '__next__')) and # output is a iterable object
              not isinstance(self.output, str)):
            args = self.output
        else: # output is a independent object
            arg = (self.output)

        for arg, src in query.items():
            src_parts = src.split(sep='.', maxsplit=2)
            
            # match argument with history context
            if src_parts == 2: 
                task_id, task_src = src_parts
                if (task_id in self.history_context and 
                    task_src in self.history_context[task_id]):
                    kwargs[arg] = self.history_context[task_id][task_src]
            else:
                # match argument with global context
                if src in self.global_context:
                    kwargs[arg] = self.global_context[src]

                # match argument with output if mapping
                if output_mapping and src in self.output.keys():
                    kwargs[arg] = self.output.__getitem__(src)

        return args, kwargs

    
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