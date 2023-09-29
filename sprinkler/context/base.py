from __future__ import annotations

from typing import Any


class Context:
    def __init__(self) -> None:
        self._context = {'local': {}, 'global': {}}

    def retrieve(self, query: dict[str, Any]) -> dict[str, Any]:
        return {
            name: self._context['local'].get(name) or self._context['global'].get(name)
            for name in query
        }
    
    def set(self, context: dict[str, Any], scope: str) -> None:
        self._context[scope].update(context)

    def delete_scope(self, scope: str) -> None:
        self._context[scope].clear()