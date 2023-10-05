from __future__ import annotations

from typing import Callable, Any
import inspect

from pydantic import create_model, BaseModel, ValidationError

from sprinkler import config


class Task:
    """The unit of operation in pipeline."""

    id: str
    input_config: dict
    _input_query: dict
    _input_model: BaseModel
    _output_model: BaseModel
    
    def __init__(
        self,
        id: str,
        operation: Callable,
        input_config: dict[str, Any | dict[str, Any]] | None = None,
        output_config: type | None = None,
    ) -> None:
        """Initialize the task class.

        Args:
            id: A task identifier. It should be following the python variable naming rule.
            operation: A callbale object defining the operation of task.
            input_config: An optional extra config for the input of the operation.
            output_config: An optional extra config for the output of the operation.
        """
        
        if not isinstance(id, str):
            raise TypeError(f'id must be str.')
        
        self.id = id

        if '__call__' not in dir(operation):
            raise TypeError(f'Task {self.id}: operation must be callable.')
        
        self.operation = operation


        operation_spec = inspect.getfullargspec(operation)
        input_config = input_config or {}

        self.input_config = {} # {argument name}: {meta data of argument ex. type, source, ...}
        self._input_query = {} # {argument name}: {source}
        
        default_len = len(operation_spec.defaults or [])

        for i, arg in enumerate(operation_spec.args):
            # if operation is instance method,
            # skip the first argument ex. self
            if i == 0 and inspect.ismethod(operation): continue            

            user_config = input_config.get(arg) or {} # configuration which user gives as input

            # {argument name}: {type}
            if not isinstance(user_config, dict):
                user_config = {'type': user_config}

            # create the base of input_config
            self.input_config[arg] = {
                'type': user_config.get('type') or operation_spec.annotations.get(arg) or Any,
                'src': user_config.get('src') or arg
            }

            if user_config.get('default'): 
                self.input_config[arg]['default'] = user_config['default']
            elif len(operation_spec.args) - i <= default_len: # when default value is given at paramter
                self.input_config[arg]['default'] = (
                    operation_spec.defaults[i - len(operation_spec.args) + default_len]
                )
            
            self._input_query[arg] = self.input_config[arg]['src']
        
        # create input pydantic model for validation
        self._input_model = create_model(
            f'TaskInput_{self.id}',
            **{
                name: (config['type'], self.input_config[name].get('default') or ...)
                for name, config in self.input_config.items()
            }
        )

        if output_config is not None:
            if issubclass(output_config, BaseModel):
                self.output_config = {
                    key: {'type': field_info.annotation}
                    for key, field_info in output_config.model_fields.items()
                }
            else:
                self.output_config = {config.DEFAULT_OUTPUT_KEY: {'type': output_config}}
        else:
            # if return type hint is None
            # output_config becomes None
            if 'return' in operation_spec.annotations:
                output_config = operation_spec.annotations['return']
                if output_config is None:
                    output_config = type(None)
            else:
                output_config = Any

            self.output_config = {config.DEFAULT_OUTPUT_KEY: {'type': output_config}}

        # create output pydantic model for validation
        self._output_model = create_model(
            f'TaskOutput_{self.id}',
            **{
                name: (config['type'], ...)
                for name, config in self.output_config.items()
            }
        )


    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.execute(*args, **kwargs)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task with given context."""
        kwargs = self._parse_input(args, kwargs)
        output = self.operation(**kwargs)
        output = self._parse_output(output)

        return output


    def _parse_input(self, args: tuple, kwargs: dict) -> dict[str, Any]:
        kwargs = inspect.getcallargs(self.operation, *args, **kwargs)

        try:
            return self._input_model.model_validate(kwargs).model_dump()
        except ValidationError as e:
            raise Exception(f'Task {self.id} input: {e}')
    
    
    def _parse_output(self, output: Any) -> Any:
        is_pydantic = True
        
        # if output is not pydantic model,
        # capsulate it to dictionary for validation
        if not issubclass(output.__class__, BaseModel): 
            is_pydantic = False
            output = {config.DEFAULT_OUTPUT_KEY: output}

        try:
            self._output_model.model_validate(output)
        except ValidationError as e:
            raise Exception(f'Task {self.id} output: {e}')
        
        if is_pydantic: 
            return output
        else:
            return output[config.DEFAULT_OUTPUT_KEY]