from __future__ import annotations

from typing import Callable, Any
import inspect
import json

from pydantic import create_model, BaseModel, ValidationError

from sprinkler.context import Context


class Task:

    DEFAULT_OUTPUT_KEY = 'return'
    
    def __init__(
        self,
        task_id: str,
        operation: Callable,
        context: dict[str, Any] | None = None,
        input_config: dict[str, Any | dict[str, Any]] | None = None,
        output_config: dict[str, Any] | BaseModel | type | str | None = None,
    ) -> None:
        
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
        self.input_query = {}
        
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
            
            self.input_query[arg] = self.input_config[arg]['src']
        

        self.input_model = create_model(
            f'TaskInput{self.task_id}',
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
        
        self.output_model = create_model(
            f'TaskOutput{self.task_id}',
            **{
                name: (config['type'], ...)
                for name, config in self.output_config.items()
            }
        )



    def execute(self, context: Context) -> None:
        context.set(self.context, 'local')
        
        args = self._parse_input(context)

        output = self.operation(**args)
        output = self._parse_output(output)

        context.delete_scope('local')
        context.set(output, 'local')

    
    def _parse_input(self, context: Context) -> dict[str, Any]:
        args = context.retrieve(self.input_query)

        try:
            self.input_model.model_validate(args)
        except ValidationError as e:
            raise Exception(f'Task {self.task_id} input: {e}')
        
        return args
    
    
    def _parse_output(self, output: Any) -> Any:
        if not isinstance(output, dict):
            output = {self.DEFAULT_OUTPUT_KEY: output}

        try:
            self.output_model.model_validate(output)
        except ValidationError as e:
            raise Exception(f'Task {self.task_id} output: {e}')

        return output