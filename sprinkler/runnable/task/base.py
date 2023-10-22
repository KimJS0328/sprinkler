from __future__ import annotations

from typing import Callable, Any, Generator, get_type_hints
from inspect import signature, Parameter, iscoroutinefunction
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import asyncio

from pydantic import create_model, BaseModel, ValidationError

from sprinkler.constants import OUTPUT_KEY, null
from sprinkler.runnable.base import Runnable
from sprinkler.context.base import Context


def _create_input_config(operation: Callable) -> OrderedDict:
    """
    """
    
    parameters = signature(operation).parameters
    annotations_ = get_type_hints(operation)

    input_config = OrderedDict()

    for param in parameters.values():

        config = annotations_.get(param.name, Any)

        if not isinstance(config, _Ann):
            config = Ann[config]

        if config.is_ctx and not config.key:
            config.key = K(param.name)

        if param.default is not Parameter.empty:
            config.default = param.default

        input_config[param.name] = config

    return input_config


def _create_output_config(operation: Callable) -> OrderedDict:
    """
    """
    
    config = get_type_hints(operation).get('return', Any)

    if not isinstance(config, _Ann):
        config = Ann[config]

    return {
        OUTPUT_KEY: config
    }


def _create_pydantic_model_cls(name: str, config: OrderedDict[str, _Ann]):
    return create_model(name, **{
        name: (ann.type, ann.default)
        for name, ann in config.items()
    })


class Task(Runnable):
    """The unit of operation in pipeline."""

    id: str = 'Unnamed Task'
    input_config: OrderedDict
    output_config: OrderedDict
    

    def __init__(
        self,
        id_: str,
        operation: Callable
    ) -> None:
        """Initialize the task class.

        Args:
            id: A task identifier. It should be following the python variable naming rule.
            operation: A callbale object defining the operation of task.
        """
        
        if not isinstance(id_, str):
            raise TypeError(f'id must be str.')
        
        self.id = id_

        if not callable(operation):
            raise TypeError(f'Task {self.id}: operation must be callable.')
        
        self.operation = operation

        self.input_config = _create_input_config(self.operation)

        self.output_config = _create_output_config(self.operation)


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
        inputs = context.query(self.input_config)
        inputs.update(kwargs)
        inputs = {
            arg: val for arg, val in inputs.items() if arg in self.input_config
        }

        remaining_params = [
            param for param in self.input_config if param not in inputs
        ]
        
        if not remaining_params:
            return inputs

        if OUTPUT_KEY in kwargs:
            input_ = kwargs[OUTPUT_KEY]

            if len(remaining_params) == 1:
                inputs[remaining_params[0]] = input_
            
            elif all(
                hasattr(input_, attr) 
                for attr in ('keys', '__getitem__', '__contains__')
            ):
                for param in remaining_params:
                    if param in input_:
                        inputs[param] = input_[param]

            elif all(
                hasattr(input_, attr) 
                for attr in ('__iter__', '__len__')
            ):
                for param, val in zip(remaining_params, input_):
                    inputs[param] = val

        else:
            input_ = args

            for param, val in zip(remaining_params, input_):
                inputs[param] = val

        return inputs


    def _validate_input(self, context: Context, args: tuple, kwargs: dict) -> dict[str, Any]:
        """Validate input arguemnts

        Returns:
            keyword arguments of validated arguments
        """
        arguments = self._bind_input(context, args, kwargs)
        input_model = _create_pydantic_model_cls(
            f'TaskInput_{self.id}', self.input_config
        )

        try:
            return (input_model
                .model_validate(arguments)
                .model_dump())
        
        except ValidationError as e:
            raise Exception(f'Task {self.id} input: {e}')
    
    
    def _validate_output(self, output: Any) -> Any:
        """Validate output

        Returns:
            validated output
        """

        output = {OUTPUT_KEY: output}
        output_model = _create_pydantic_model_cls(
            f'TaskOutput_{self.id}', self.output_config
        )

        try:
            return (output_model
                .model_validate(output)
                .model_dump()[OUTPUT_KEY])
        
        except ValidationError as e:
            raise Exception(f'Task {self.id} output: {e}')


class K:

    key: tuple

    def __init__(self, *args) -> None:
        self.key = args

    def __iter__(self):
        return iter(self)
    
    def __bool__(self):
        return bool(self.key)


class _Ann:

    type: Any = null
    key: Any = null
    _default: Any = null
    _is_ctx: bool


    def __init__(self, is_ctx) -> None:
        self._is_ctx = is_ctx


    @property
    def default(self):
        return self._default if self._default is not null else ...
    

    @default.setter
    def default(self, value):
        self._default = value

    
    @property
    def is_ctx(self):
        return self._is_ctx


    def __getitem__(self, config):
        ann = Ann(self.is_ctx)
        if isinstance(config, tuple):
            ann.type = config[0]
            ann.key = config[1] if len(config) > 1 else K()
        else:
            ann.type = config
            ann.key = K()

        if ann.type is None:
            ann.type = type(None)
        if not isinstance(ann.key, K):
            ann.key = K(ann.key)

        return ann
    
Ann = _Ann(False)
Ctx = _Ann(True)