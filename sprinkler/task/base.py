from __future__ import annotations

from typing import Callable, Any
import inspect

from pydantic import create_model, BaseModel, ValidationError

from sprinkler.context import Context


class Task:
    """The unit of operation in pipeline."""

    DEFAULT_OUTPUT_KEY = 'return'
    
    def __init__(
        self,
        task_id: str,
        operation: Callable,
        context: dict[str, Any] | None = None,
        input_config: dict[str, Any | dict[str, Any]] | None = None,
        output_config: dict[str, Any] | Any | None = None,
    ) -> None:
        """Initialize the task class.

        Args:
            task_id: A task identifier. It should be following the python variable naming rule.
            operation: A callbale object defining the operation of task.
            context: An optional local context for this task. It is used only in this task.
            input_config: An optional extra config for the input of the operation.
            output_config: An optional extra config for the output of the operation.
        
        """
        
        if not isinstance(task_id, str):
            raise TypeError(f'task_id must be str.')
        
        self.task_id = task_id


        if '__call__' not in dir(operation):
            raise TypeError(f'Task {self.task_id}: operation must be callable.')
        
        self.operation = operation


        for key in context:
            if not isinstance(key, str):
                raise TypeError((
                    f'Task {self.task_id}: '
                    'key of context must be str.'
                ))
            
        self.context = context or {}


        operation_spec = inspect.getfullargspec(operation)
        input_config = input_config or {}

        self.input_config = {}
        self._input_query = {}
        
        default_len = len(operation_spec.defaults or [])

        for i, arg in enumerate(operation_spec.args):
            user_config = input_config.get(arg) or {}

            if not isinstance(user_config, dict):
                user_config = {'type': user_config}

            self.input_config[arg] = {
                'type': user_config.get('type') or operation_spec.annotations.get(arg) or Any,
                'src': user_config.get('src') or arg
            }

            if user_config.get('default'):
                self.input_config[arg]['default'] = user_config['default']
            elif len(operation_spec.args) - i <= default_len:
                self.input_config[arg]['default'] = (
                    operation_spec.defaults[i - len(operation_spec.args) + default_len]
                )
            
            self._input_query[arg] = self.input_config[arg]['src']
        

        self._input_model = create_model(
            f'TaskInput_{self.task_id}',
            **{
                name: (config['type'], self.input_config[name].get('default') or ...)
                for name, config in self.input_config.items()
            }
        )


        output_config = output_config or operation_spec.annotations.get('return') or Any

        if issubclass(output_config.__class__, BaseModel):
            self.output_config = {
                key: {'type': field_info.annotation}
                for key, field_info in output_config.model_fields.items()
            }
        else:
            self.output_config = {self.DEFAULT_OUTPUT_KEY: {'type': output_config}}
        
        self._output_model = create_model(
            f'TaskOutput_{self.task_id}',
            **{
                name: (config['type'], ...)
                for name, config in self.output_config.items()
            }
        )


    def execute(self, context: Context) -> None:
        """Execute the task given context.

        Args:
            context: The context of the pipeline.

        """
        context.replace_local_context(self.context)
        
        kwargs = self._parse_input(context)

        output = self.operation(**kwargs)
        output = self._parse_output(output)

        context.replace_output_context(output)

    
    def _parse_input(self, context: Context) -> dict[str, Any]:
        kwargs = context.get_values(self._input_query)

        remaining_keys = self.input_config.keys() - kwargs.keys()
        prev_output = context.get_values({
            self.DEFAULT_OUTPUT_KEY: self.DEFAULT_OUTPUT_KEY
        })

        if len(remaining_keys) == 1:
            remaining_key = list(remaining_keys)[0]
            value = (
                prev_output.get(self.DEFAULT_OUTPUT_KEY)
                or self.input_config[remaining_key].get('default')
            )

            if value is not None:
                kwargs[remaining_key] = value

        try:
            self._input_model.model_validate(kwargs)
            return kwargs
        except ValidationError as e:
            raise Exception(f'Task {self.task_id} input: {e}')
    
    
    def _parse_output(self, output: Any) -> Any:
        if not isinstance(output, dict):
            output = {self.DEFAULT_OUTPUT_KEY: output}

        try:
            self._output_model.model_validate(output)
        except ValidationError as e:
            raise Exception(f'Task {self.task_id} output: {e}')

        return output