from __future__ import annotations

from typing import Callable, Any, OrderedDict, get_type_hints
from inspect import Signature, Parameter

from pydantic import create_model, BaseModel, ValidationError

from sprinkler import config
from sprinkler.runnable.base import Runnable


def _none_if_empty(value: Any) -> Any:
    return None if value is Parameter.empty else value


def _create_input_config_and_query(
    parameters: OrderedDict[str, Parameter],
    user_config: dict
) -> tuple[dict, dict]:
    """
    
    """

    input_config = {} # {argument name}: {meta data of argument ex. type, source, ...} 
    input_query = [] # [({argument name}, {source})]

    for param in parameters.values():

        param_config = user_config.get(param.name, {})

        if not isinstance(param_config, dict):
            param_config = {'type': param_config}

        # create the base of input_config
        input_config[param.name] = {
            'type': param_config.get('type') or _none_if_empty(param.annotation) or Any,
            'src': param_config.get('src') or param.name,
        }

        default = param_config.get('default') or _none_if_empty(param.default)

        if default is not None:
            input_config[param.name]['default'] = default

        input_query.append((param.name, input_config[param.name]['src']))

    return input_config, input_query



def _create_output_config(
    operation: Any,
    user_config: Any
) -> dict:
    """
    
    """
    annotations_ = get_type_hints(operation)

    if user_config is not None:
        output_type = user_config

    elif 'return' in annotations_:
        output_type = annotations_['return']

    else:
        output_type = Any

    return {
        config.OUTPUT_KEY: {
            'type': output_type
        }
    }

class Task(Runnable):
    """The unit of operation in pipeline."""

    id: str = 'Unnamed Task'
    input_config: dict = {}
    _input_query: list[tuple[str, str]] = {}
    _input_model: BaseModel
    _output_model: BaseModel
    

    def __init__(
        self,
        id_: str,
        operation: Callable,
        *,
        input_config: dict[str, Any | dict[str, Any]] | None = None,
        output_config: dict[str, Any] | Any | None = None,
    ) -> None:
        """Initialize the task class.

        Args:
            id: A task identifier. It should be following the python variable naming rule.
            operation: A callbale object defining the operation of task.
            input_config: An optional extra config for the input of the operation.
            output_config: An optional extra config for the output of the operation.
        """
        
        if not isinstance(id_, str):
            raise TypeError(f'id must be str.')
        
        self.id = id_

        if '__call__' not in dir(operation):
            raise TypeError(f'Task {self.id}: operation must be callable.')
        
        self.operation = operation

        self.operation_signature = Signature.from_callable(operation)

        self.input_config, self._input_query = _create_input_config_and_query(
            self.operation_signature.parameters, 
            input_config or {}
        )
        
        # create input pydantic model for validation
        self._input_model = create_model(
            f'TaskInput_{self.id}',
            **{
                name: (config['type'], config.get('default') or ...)
                for name, config in self.input_config.items()
            }
        )

        self.output_config = _create_output_config(operation, output_config)

        # create output pydantic model for validation
        self._output_model = create_model(
            f'TaskOutput_{self.id}',
            **{
                name: (config['type'], ...)
                for name, config in self.output_config.items()
            }
        )


    def __call__(self, *args, **kwargs) -> Any:
        return self.run(*args, **kwargs)


    def run(self, *args, **kwargs) -> Any:
        """run the task with given context."""
        kwargs = self._validate_input(args, kwargs)
        output = self.operation(**kwargs)
        output = self._validate_output(output)

        return output


    def _validate_input(self, args: tuple, kwargs: dict) -> dict[str, Any]:
        """Validate input arguemnts

        Returns:
            keyword arguments of validated arguments
        """
        kwargs = self.operation_signature.bind_partial(*args, **kwargs).arguments

        try:
            return self._input_model.model_validate(kwargs).model_dump()
        except ValidationError as e:
            raise Exception(f'Task {self.id} input: {e}')
    
    
    def _validate_output(self, output: Any) -> Any:
        """Validate output

        Returns:
            validated output
        """

        output = {config.OUTPUT_KEY: output}

        try:
            return (self._output_model
                .model_validate(output)
                .model_dump()[config.OUTPUT_KEY])
        
        except ValidationError as e:
            raise Exception(f'Task {self.id} output: {e}')


    def get_query(self) -> list[tuple[str, str]]:
        return self._input_query