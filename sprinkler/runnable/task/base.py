from __future__ import annotations

from typing import Callable, Any, Generator, get_type_hints
from inspect import signature, Parameter, iscoroutinefunction
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import asyncio

from pydantic import create_model, BaseModel, ValidationError

from sprinkler import config
from sprinkler.runnable.base import Runnable
from sprinkler.context.base import Context


def _none_if_empty(value: Any) -> Any:
    return None if value is Parameter.empty else value


def _create_input_config_and_query(
    operation: Callable,
    user_config: dict
) -> tuple[dict, dict]:
    """
    
    """
    
    parameters = signature(operation).parameters
    annotations_ = get_type_hints(operation)

    input_config = {} # {argument name}: {meta data of argument ex. type, source, ...} 
    input_query = OrderedDict() # {argument name}: {source}

    for param in parameters.values():

        param_config = user_config.get(param.name, {})

        if not isinstance(param_config, dict):
            param_config = {'type': param_config}

        # create the base of input_config
        input_config[param.name] = {
            'type': param_config.get('type') or annotations_.get(param.name) or Any,
            'src': param_config.get('src') or param.name
        }

        default = param_config.get('default') or _none_if_empty(param.default)

        if default is not None:
            input_config[param.name]['default'] = default

        input_query[param.name] = input_config[param.name]['src']

    return input_config, input_query


def _create_output_config(
    operation: Callable,
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
    _input_query: OrderedDict[str, str] = OrderedDict()
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

        if not callable(operation):
            raise TypeError(f'Task {self.id}: operation must be callable.')
        
        self.operation = operation

        self.input_config, self._input_query = _create_input_config_and_query(
            self.operation, 
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

        self.output_config = _create_output_config(self.operation, output_config)

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
    

    def _generator_for_run(
        self,
        context_: dict[str, Any] | Context,
        args: tuple,
        kwargs: dict
    ) -> Generator[dict[str, Any], Any, Any]:
        
        context = context_

        if isinstance(context_, dict):
            context = Context()
            context.add_global(context_)
        
        input_ = self._validate_input(context, args, kwargs)
        output = yield input_
        output = self._validate_output(output)

        return output


    def run(self, *args, **kwargs) -> Any:
        """Run the task synchronously."""
        return self.run_with_context({}, *args, **kwargs)


    def run_with_context(
        self,
        context_: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        """Run the task with given context synchronously."""

        gen = self._generator_for_run(context_, args, kwargs)
        input_ = next(gen)
        try:
            gen.send(self._run_operation(input_))
        except StopIteration as output:
            return output.value
    

    def _run_operation(self, input_: dict[str, Any]) -> Any:
        if iscoroutinefunction(self.operation):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    Task._run_coroutine_in_another_thread,
                    self.operation,
                    input_
                )
                return future.result()
        else:
            return self.operation(**input_)


    @staticmethod
    def _run_coroutine_in_another_thread(coro, input_) -> Any:
        return asyncio.run(coro(**input_))


    async def arun(self, *args, **kwargs) -> Any:
        """run the task with given context."""
        return await self.arun_with_context({}, *args, **kwargs)


    async def arun_with_context(
        self,
        context_: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        
        gen = self._generator_for_run(context_, args, kwargs)
        input_ = next(gen)
        try: 
            gen.send(await self._arun_operation(input_))
        except StopIteration as output:
            return output.value


    async def _arun_operation(self, input_: dict[str, Any]) -> Any:
        if iscoroutinefunction(self.operation):
            return await self.operation(**input_)
        else:
            return self.operation(**input_)


    def _bind_input(self, context: Context, args: tuple, kwargs: dict) -> dict[str, Any]:
        arguments = context.get_kwargs(self._input_query)
        arguments.update(kwargs)
        arguments = {arg: val for arg, val in arguments.items() if arg in self._input_query}

        remaining_params = [param for param in self._input_query if param not in arguments]
        
        if not remaining_params:
            return arguments

        if config.OUTPUT_KEY in kwargs:
            input_ = kwargs[config.OUTPUT_KEY]

            if len(remaining_params) == 1:
                arguments[remaining_params[0]] = input_
            
            elif all(
                hasattr(input_, attr) 
                for attr in ('keys', '__getitem__', '__contains__')
            ):
                for param in remaining_params:
                    if param in input_:
                        arguments[param] = input_[param]

            elif all(
                hasattr(input_, attr) 
                for attr in ('__iter__', '__len__')
            ):
                for param, val in zip(remaining_params, input_):
                    arguments[param] = val

        else:
            input_ = args

            for param, val in zip(remaining_params, input_):
                arguments[param] = val

        return arguments


    def _validate_input(self, context: Context, args: tuple, kwargs: dict) -> dict[str, Any]:
        """Validate input arguemnts

        Returns:
            keyword arguments of validated arguments
        """
        arguments = self._bind_input(context, args, kwargs)

        try:
            return (self._input_model
                .model_validate(arguments)
                .model_dump())
        
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
