from __future__ import annotations

from typing import Any


class Context:
    def __init__(self) -> None:
        self._context = {'local': {}, 'global': {}}

    def retrieve(self, query: dict[str, Any]) -> dict[str, Any]:
        result = {}
        for key in query:
            value = self._context['local'].get(key) or self._context['global'].get(key)
            if value is not None:
                result[key] = value
        return result
    
    def set(self, context: dict[str, Any], scope: str) -> None:
        self._context[scope].update(context)

    def delete_scope(self, scope: str) -> None:
        self._context[scope].clear()