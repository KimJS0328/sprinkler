from __future__ import annotations

from typing import Callable, Tuple, Dict, Any
import inspect

from pydantic import create_model, BaseModel, ValidationError

from sprinkler import config

def create_input_config_and_query(
        operation: Callable,
        user_config: Dict
) -> Tuple[Dict, Dict]:
    """
    
    """
    operation_spec = inspect.getfullargspec(operation)
    default_len = len(operation_spec.defaults or [])

    input_config = {} # {argument name}: {meta data of argument ex. type, source, ...} 
    input_query = {} # {argument name}: {source}

    for i, arg in enumerate(operation_spec.args):
        # if operation is instance method,
        # skip the first argument ex. self
        if i == 0 and inspect.ismethod(operation): continue            

        # configuration of the argument which user gives as input
        user_config_arg = user_config.get(arg, {})

        # {argument name}: {type}
        if not isinstance(user_config_arg, dict):
            user_config_arg = {'type': user_config_arg}

        # create the base of input_config
        input_config[arg] = {
            'type': user_config_arg.get('type') or operation_spec.annotations.get(arg) or Any,
            'src': user_config_arg.get('src') or arg
        }

        if user_config_arg.get('default'): 
            input_config[arg]['default'] = user_config_arg['default']
        elif len(operation_spec.args) - i <= default_len: # when default value is given at paramter
            input_config[arg]['default'] = (
                operation_spec.defaults[i - len(operation_spec.args) + default_len]
            )
        
        input_query[arg] = input_config[arg]['src']
    
    return input_config, input_query


def create_output_config(
        operation: Callable,
        user_config: type
) -> Dict:
    """
    
    """
    annotations = inspect.getfullargspec(operation).annotations

    if user_config is not None:
        if issubclass(user_config, BaseModel):
            return {
                key: {'type': field_info.annotation}
                for key, field_info in user_config.model_fields.items()
            }
        else:
            return {config.DEFAULT_OUTPUT_KEY: {'type': user_config}}
    else:
        # if return type hint is None
        # user_config becomes None
        if 'return' in annotations:
            user_config = annotations['return']
            if user_config is None:
                user_config = type(None)
        else:
            user_config = Any

        return {config.DEFAULT_OUTPUT_KEY: {'type': user_config}}


class Task:
    """The unit of operation in pipeline."""

    id: str = 'Unnamed Task'
    input_config: dict = {}
    _input_query: dict = {}
    _input_model: BaseModel
    _output_model: BaseModel
    
    def __init__(
        self,
        id_: str,
        operation: Callable,
        input_config: Dict[str, Any | Dict[str, Any]] | None = None,
        output_config: type | None = None,
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

        self.input_config, self._input_query = create_input_config_and_query(operation, input_config or {})
        
        # create input pydantic model for validation
        self._input_model = create_model(
            f'TaskInput_{self.id}',
            **{
                name: (config['type'], self.input_config[name].get('default') or ...)
                for name, config in self.input_config.items()
            }
        )

        self.output_config = create_output_config(operation, output_config)

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
        kwargs = self._parse_input(args, kwargs)
        output = self.operation(**kwargs)
        output = self._parse_output(output)

        return output


    def _parse_input(self, args: tuple, kwargs: dict) -> dict[str, Any]:
        """Validate input arguemnts

        Returns:
            keyword arguments of validated arguments
        """
        kwargs = inspect.getcallargs(self.operation, *args, **kwargs)

        try:
            return self._input_model.model_validate(kwargs).model_dump()
        except ValidationError as e:
            raise Exception(f'Task {self.id} input: {e}')
    
    
    def _parse_output(self, output: Any) -> Any:
        """Validate output

        Returns:
            validated output
        """
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
        
    def get_query(self):
        return self._input_query