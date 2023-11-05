from __future__ import annotations

from concurrent.futures import Executor
from typing import Any, Generator
from functools import partial
import copy

from sprinkler.runnable.base import Runnable
from sprinkler.context import Context
from sprinkler.constants import OUTPUT_KEY


class Pipeline(Runnable):
    """The pipeline which executes `Runnable` serially

    Attributes:
        id: identifer of `Runnable`.
        members: the list of `Runnable`.
        member_id_set: the set which contains id of `Runnable`
        context: the global context values for pipeline instance
    """

    id: str
    members: list[Runnable]
    member_id_set: set[str]
    context: Context

    def __init__(
        self,
        id_: str,
        *,
        context: dict[str, Any] | None = None
    ) -> None:
        """Initializes the pipeline instance with context

        Args:
            context: 
        """
        self.id = id_
        self.members= []
        self.member_id_set = set()
        self.context = Context()
        
        if context:
            self.context.add_global(context)
        

    def add(self, *args: Runnable) -> Pipeline:
        """Add the `Runnable` instance to pipeline

        Args:
            runnable: the Runnable instance
        """
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
        context: dict[str, Any], 
        args: tuple,
        kwargs: dict,
        method_name: str
    ) -> Generator[Any, Any, None]:
        
        context_for_run = copy.deepcopy(self.context)

        # add to global context in pipeline
        if isinstance(context, dict):
            context_for_run.add_global(context)
        elif isinstance(context, Context):
            context_for_run.update(context)

        # run tasks as chain with context
        for runnable in self.members:
            func = getattr(runnable, method_name)
            output = yield partial(
                func,
                context_for_run,
                *args,
                **kwargs
            )
            context_for_run.add_history(output, runnable.id)
            args = ()
            kwargs = {OUTPUT_KEY: output}


    def run(
        self,
        *args,
        __executor__: Executor | None = None,
        **kwargs
    ) -> Any:
        """run the pipeline flows

        excute the tasks in the pipeline sequentially

        Returns:
            final output of pipeline
        """
        return self.run_with_context(
            {},
            *args,
            __executor__=__executor__,
            **kwargs
        )
    

    def run_with_context(
        self,
        context: dict[str, Any],
        *args,
        __executor__: Executor | None = None,
        **kwargs
    ) -> Any:

        gen = self._generator_for_run(
            context, args, kwargs, 'run_with_context'
        )
        output = None
        
        while True:
            try:
                output = gen.send(output)(__executor__=__executor__)
            except StopIteration:
                break

        return output


    async def arun(
        self,
        *args,
        **kwargs
    ) -> Any:
        return await self.arun_with_context(
            {},
            *args,
            **kwargs
        )


    async def arun_with_context(
        self,
        context: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        
        gen = self._generator_for_run(
            context, args, kwargs, 'arun_with_context'
        )
        output = None
        
        while True:
            try:
                output = await gen.send(output)()
            except StopIteration:
                break

        return output


    def __call__(
        self,
        *args,
        __executor__: Executor | None = None,
        **kwargs
    ) -> Any:
        return self.run(
            *args,
            __executor__=__executor__,
            **kwargs
        )


    def make_graph(self, parent=None) -> Any:
        from pygraphviz import AGraph

        if parent is None:
            graph = AGraph(directed=True, name=f'cluster_{self.id}', label=self.id, compound='true')
        else:
            graph = parent.add_subgraph(name=f'cluster_{self.id}', label=self.id)

        prev_last = None

        for runnable in self.members:
            child = runnable.make_graph(graph)

            if prev_last is not None:
                attrs = {}
                if prev_last.name is not None:
                    attrs['ltail'] = prev_last.name
                if child.name is not None:
                    attrs['lhead'] = child.name

                graph.add_edge(
                    prev_last.nodes()[-1],
                    child.nodes()[-1],
                    **attrs
                )
            prev_last = child
        
        return graph