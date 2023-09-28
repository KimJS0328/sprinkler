from typing import Callable, Any
from pydoc import locate

from sprinkler.context import Context

class Task:

    _DEFAULT_OUTPUT_KEY = '__output__'
    
    def __init__(
        self,
        input_: dict[str, Any],
        output: dict[str, Any] | str,
        context: dict[str, Any],
        operation: Callable
    ) -> None:
        self.input = input_
        self.output = output
        self.context = context
        self.operation = operation

        if isinstance(self.output, str):
            self.output = {self._DEFAULT_OUTPUT_KEY: self.output}

        self._src_query: dict[str, Any] = {}
        self._common_query: dict[str, Any] = {}

        for name, config in self.input.items():
            if isinstance(config, dict):
                self._src_query[name] = config
            else:
                self._common_query[name] = config


    def execute(self, context: Context) -> None:
        context.set(self.context, 'local')
        
        args = self._parse_input(context)

        output = self.operation(**args)
        output = self._parse_output(output)

        context.delete_scope('local')
        context.set(output, 'local')

    
    def _parse_input(self, context: Context) -> dict[str, Any]:
        args = context.retrieve(self.input)

        if len(self.input) == 1:
            for name in self.input:
                args[name] = context.retrieve({self._DEFAULT_OUTPUT_KEY: ''})[self._DEFAULT_OUTPUT_KEY]
                
        
        for name, config in self.input.items():
            type_ = locate(config['type'] if isinstance(config, dict) else config)

            if args.get(name) is None:
                raise ValueError(f'The input argument "{name}" cannot be found')

            elif not isinstance(args[name], type_):
                raise TypeError(f'{name} should be {type_.__name__} type, but {type(args[name]).__name__} type')
        
        return args
    
    
    def _parse_output(self, output: Any) -> Any:
        if not isinstance(output, dict):
            output = {self._DEFAULT_OUTPUT_KEY: output}
        return output