from typing import Callable, Any
from pydoc import locate

from sprinkler.context import Context

class Task:

    _DEFAULT_OUTPUT_KEY = '__output__'
    
    def __init__(
        self,
        task_id: str,
        input_signature: dict[str, Any],
        output_signature: dict[str, Any] | type,
        context: dict[str, Any],
        operation: Callable,
    ) -> None:
        self.task_id = task_id
        self.input_signature = input_signature
        self.output_signature = output_signature
        self.context = context
        self.operation = operation

        self._src_query = {}

        for name, config in self.input_signature.items():
            if isinstance(config, dict):
                self.input_signature[name]['type'] = locate(config['type'])
                if config.get('src') is not None:
                    self._src_query[name] = self.input_signature[name]
            else:
                self.input_signature[name] = {'type': locate(config)}

        if isinstance(self.output_signature, str):
            self.output_signature = {self._DEFAULT_OUTPUT_KEY: self.output_signature}

        self.output_signature = {
            name: locate(config) if isinstance(config, str) else config 
            for name, config in self.output_signature.items()
        }


    def execute(self, context: Context) -> None:
        context.set(self.context, 'local')
        
        args = self._parse_input(context)

        output = self.operation(**args)
        output = self._parse_output(output)

        context.delete_scope('local')
        context.set(output, 'local')

    
    def _parse_input(self, context: Context) -> dict[str, Any]:
        args = context.retrieve(self.input_signature)

        if len(self.input_signature) == 1:
            for name in self.input_signature:
                args[name] = context.retrieve({self._DEFAULT_OUTPUT_KEY: ''})[self._DEFAULT_OUTPUT_KEY]
                
        
        for name, config in self.input_signature.items():
            type_ = config['type']

            if args.get(name) is None:
                raise NameError((
                    f'Task {self.task_id}: '
                    f'The input argument "{name}" cannot be found in the context'
                ))

            elif not isinstance(args[name], type_):
                raise TypeError((
                    f'Task {self.task_id}: '
                    f'The input argument {name} should be {type_.__name__} type, '
                    f'but {type(args[name]).__name__} type'
                ))
        
        return args
    
    
    def _parse_output(self, output: Any) -> Any:
        if not isinstance(output, dict):
            output = {self._DEFAULT_OUTPUT_KEY: output}

        for name in output:
            if self.output_signature.get(name) is None:
                raise NameError((
                    f'Task {self.task_id}: '
                    f'The output {name} cannot be found in the context'    
                ))
            elif not isinstance(output[name], self.output_signature[name]):
                raise TypeError((
                    f'Task {self.task_id}: '
                    f'The output {name} should be '
                    f'{self.output_signature[name].__name__} type, '
                    f'but {type(output[name]).__name__} type'
                ))

        return output