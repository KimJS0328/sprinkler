from __future__ import annotations

from typing import Callable, Any, Generator
from inspect import Parameter, iscoroutinefunction, Signature
from collections import OrderedDict
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
import asyncio
import copy

from pydantic import create_model, ValidationError, ConfigDict

from sprinkler.constants import OUTPUT_KEY, null
from sprinkler.utils import recursive_search, distribute_value
from sprinkler.runnable.base import Runnable
from sprinkler.context.base import Context


class Task(Runnable):
    """The unit of operation in pipeline."""

    id: str = 'Unnamed Task'
    operation: Callable
    context: Context
    _input_model_config: dict[str, tuple]
    _output_model_config: dict[str, tuple]
    _param_with_key: dict[K, list[str]]
    _ctx_with_key: dict[K, list[str]]
    

    def __init__(
        self,
        id_: str,
        operation: Callable | None = None,
        *,
        context: dict[str, Any] | None = None
    ) -> None:
        """Initialize the task class.

        Args:
            id: A task identifier. It should be following the python variable naming rule.
            operation: A callbale object defining the operation of task.
        """
        
        if not isinstance(id_, str):
            raise TypeError(f'id must be str.')
        
        self.id = id_
        self.operation = operation
        self.context = Context()

        if context:
            self.context.add_global(context)

        if self.operation is not None:
            self._set_operation_config()


    def _set_operation_config(self):
        if not callable(self.operation):
            raise TypeError(f'Task {self.id}: operation must be callable.')

        signature = Signature.from_callable(self.operation)

        self._set_input_config(signature.parameters)
        self._set_output_config(signature.return_annotation)


    def _set_input_config(self, params: OrderedDict[str, Parameter]):
        self._input_model_config = {}
        self._param_with_key = {}
        self._ctx_with_key = {}

        for param in params.values():
            config = self._parse_annotation(
                param.name, param.annotation
            )

            if param.default is not Parameter.empty:
                config.default = param.default

            self._input_model_config[param.name] = (config.type, config.default)

            if config.is_ctx:
                if config.key in self._ctx_with_key:
                    self._ctx_with_key[config.key].append(param.name)
                else:
                    self._ctx_with_key[config.key] = [param.name]
            else:
                if config.key in self._param_with_key:
                    self._param_with_key[config.key].append(param.name)
                else:
                    self._param_with_key[config.key] = [param.name]


    def _set_output_config(self, return_ann: Any):
        config = self._parse_annotation(
            '', return_ann
        )
        
        self._output_model_config = {
            OUTPUT_KEY: (config.type, ...)
        }


    def _parse_annotation(self, param_name: str, ann: Any) -> Any:
        ann = ann if ann is not Parameter.empty else Any

        if isinstance(ann, str):
            ann = eval(ann, self.operation.__globals__)
        
        if not isinstance(ann, _Ann):
            ann = Ann[ann]
        if ann.is_ctx and not ann.key:
            ann.key = K(param_name)

        return ann


    def __call__(self, *args, **kwargs) -> Any:
        if self.operation is None:
            self.operation = args[0]
            self._set_operation_config()
            return self
        else:
            return self.run(*args, **kwargs)


    def _generator_for_run(
        self,
        context_: dict[str, Any] | Context,
        args: tuple,
        kwargs: dict
    ) -> Generator[dict[str, Any], Any, Any]:
        
        context_for_run = copy.deepcopy(self.context)

        if isinstance(context_, dict):
            context_for_run.add_global(context_)
        elif isinstance(context_, Context):
            context_for_run.update(context_)
        
        input_ = self._validate_input(context_for_run, args, kwargs)
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
        ctx = context.query(self._ctx_with_key.keys())
        input_ = {}

        for key, value in ctx.items():
            input_.update(distribute_value(
                self._ctx_with_key[key],
                value
            ))

        if OUTPUT_KEY in kwargs:
            target = kwargs[OUTPUT_KEY]
            for key, params in self._param_with_key.items():
                result = recursive_search(key, target) if key else target
                if result is not null:
                    input_.update(distribute_value(
                        params, result
                    ))

        else:
            params = chain.from_iterable(self._param_with_key.values())

            for param, arg in zip(params, args):
                input_[param] = arg
                
            input_.update(kwargs)

        return input_


    def _validate_input(self, context: Context, args: tuple, kwargs: dict) -> dict[str, Any]:
        """Validate input arguemnts

        Returns:
            keyword arguments of validated arguments
        """
        arguments = self._bind_input(context, args, kwargs)
       
        input_model = create_model(
            f'TaskInput_{self.id}',
            **self._input_model_config,
            __config__=ConfigDict(arbitrary_types_allowed=True)
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
        output_model = create_model(
            f'TaskOutput_{self.id}',
            **self._output_model_config,
            __config__=ConfigDict(arbitrary_types_allowed=True)
        )

        try:
            return (output_model
                .model_validate(output)
                .model_dump()[OUTPUT_KEY])
        
        except ValidationError as e:
            raise Exception(f'Task {self.id} output: {e}')


    def make_graph(self, parent=None) -> Any:
        from pygraphviz import AGraph

        if parent is None:
            graph = AGraph()
        else:
            graph = parent.add_subgraph()
            
        graph.add_node(self.id)

        return graph


class K:

    key: tuple

    def __init__(self, *args) -> None:
        self.key = args

    def __iter__(self):
        return iter(self.key)
    
    def __bool__(self):
        return bool(self.key)
    
    def __eq__(self, __value: object) -> bool:
        return self.key == __value.key
    
    def __hash__(self) -> int:
        return hash(self.key)


class _Ann:

    type: Any = null
    key: Any = null
    default: Any = ...
    is_ctx: bool

    def __eq__(self, __value: object) -> bool:
        return (
            self.type == __value.type
            and self.key == __value.key
            and self.default == __value.default
            and self.is_ctx == __value.is_ctx
        )

    def __init__(self, is_ctx) -> None:
        self.is_ctx = is_ctx

    def __getitem__(self, config):
        ann = _Ann(self.is_ctx)

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