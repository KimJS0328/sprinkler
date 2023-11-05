from __future__ import annotations

from typing import Any, Generator
from functools import partial
from concurrent.futures import (
    ThreadPoolExecutor,
    Executor
)
import copy
import asyncio

from sprinkler.runnable.base import Runnable
from sprinkler.context.base import Context
from sprinkler.constants import OUTPUT_KEY


class Group(Runnable):
    """The group of parallel running `Runnable`"""

    id: str
    members: list[Runnable]
    member_id_set: set[str]
    context: Context
    executor_type: str
    executor_kwargs: dict


    def __init__(
        self,
        id_: str,
        *,
        context: dict[str, Any] = None
    ) -> None:
        """Initialize the Group with context.
        
        Args:
            context: 
        """
        self.id = id_
        self.members = []
        self.member_id_set = set()
        self.context = Context()

        if context:
            self.context.add_global(context)


    def add(self, *args: Runnable) -> Group:
        """Add new `Runnable` instance to this group"""
        for runnable in args:
            if not isinstance(runnable, Runnable):
                raise TypeError('Given task parameter is not `Runnable` instance')
            
            if runnable.id in self.member_id_set:
                raise Exception(f'`Runnable` \'{runnable.id}\' is already exsists')
            
            self.members.append(runnable)
            self.member_id_set.add(runnable.id)

        return self


    def _generator_for_run(
        self, 
        context_: dict[str, Any] | Context,
        inputs: dict[str, Any],
        default: Any,
        method_name: str
    ) -> Generator[tuple[str, Any], None, None]:
        
        context_for_run = copy.deepcopy(self.context)

        if isinstance(context_, dict):
            context_for_run.add_global(context_)
        elif isinstance(context_, Context):
            context_for_run.update(context_)

        if OUTPUT_KEY in inputs:
            default = inputs[OUTPUT_KEY]

        for runnable in self.members:
            input_ = inputs.get(runnable.id, default)

            func = partial(
                getattr(runnable, method_name), 
                context_for_run,
                **{OUTPUT_KEY: input_}
            )

            yield runnable.id, func


    def run(
        self,
        *,
        __executor__: Executor | None = None,
        __default__: Any = None,
        **inputs
    ) -> dict[str, Any]:
        """
        """
        return self.run_with_context(
            {},
            __executor__=__executor__,
            __default__=__default__,
            **inputs
        )


    def run_with_context(
        self, 
        context: dict[str, Any] | Context,
        *,
        __executor__: Executor | None = None,
        __default__: Any = None,
        **inputs
    ) -> dict[str, Any]:

        needs_shutdown = False

        if __executor__ is None:
            __executor__ = ThreadPoolExecutor()
            needs_shutdown = True
        
        if __executor__ == 'asyncio':
            results = asyncio.run(self.arun_with_context(
                context, __default__=__default__, **inputs
            ))

        else:
            gen = self._generator_for_run(
                context, inputs, __default__, 'run_with_context'
            )
            results = {}

            for id_, func in gen:
                future = __executor__.submit(func, __executor__='asyncio')
                results[id_] = future
            
            if needs_shutdown:
                __executor__.shutdown()
            
            results = {id_: future.result() for id_, future in results.items()}

        return results


    async def arun(
        self,
        *,
        __default__: Any = None,
        **inputs
    ) -> Any:
        return await self.arun_with_context(
            {},
            __default__=__default__,
            **inputs
        )


    async def arun_with_context(
        self, 
        context_: dict[str, Any] | Context,
        *,
        __default__: Any = None,
        **inputs
    ) -> Any:

        results = await asyncio.gather(*[
            func() for _, func in self._generator_for_run(
                context_, inputs, __default__, 'arun_with_context'
            )
        ])
        
        results = {
            self.members[i].id: result 
            for i, result in enumerate(results)
        }
        
        return results


    def __call__(
        self,
        *,
        __executor__: Executor | None = None,
        __default__: Any = None,
        **inputs
    ) -> Any:
        return self.run(
            __executor__=__executor__,
            __default__=__default__,
            **inputs
        )


    def make_graph(self, parent=None) -> Any:
        from pygraphviz import AGraph

        if parent is None:
            graph = AGraph(directed=True, name=f'cluster_{self.id}', label=self.id, compound='true')
        else:
            graph = parent.add_subgraph(name=f'cluster_{self.id}', label=self.id)

        for runnable in self.members:
            runnable.make_graph(graph)
        
        return graph