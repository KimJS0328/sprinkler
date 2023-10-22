from typing import Any, List, Dict
from collections.abc import Iterable

from sprinkler.constants import null


def recursive_search(key: Iterable, target: Any) -> Any:
    for k in key:
        try:
            target = target[k]
        except (TypeError, KeyError):
            return null
        
    return target


def distribute_value(targets: List[str], value: Any) -> Dict[str, Any]:
    result = {}

    if len(targets) != 1 and isinstance(value, Iterable):
        for t, v in zip(targets, value):
            result[t] = v
    else:
        for t in targets:
            result[t] = value

    return result