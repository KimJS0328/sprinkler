from __future__ import annotations

from typing import Any

from sprinkler.context.base import Context


class Runnable:

    id: str

    def run(self, *args, **kwargs) -> Any:
        raise NotImplementedError


    def run_with_context(
        self,
        context_: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        raise NotImplementedError
    

    async def arun(self, *args, **kwargs) -> Any:
        raise NotImplementedError


    async def arun_with_context(
        self,
        context_: dict[str, Any] | Context,
        *args,
        **kwargs
    ) -> Any:
        raise NotImplementedError
    

    def graphviz(self, parent=None) -> Any:
        raise NotImplementedError
    

    def visualize(self, file_path: str) -> None:
        graph = self.graphviz()

        graph.layout(prog='dot', args='-Nshape=box')
        graph.draw(file_path)