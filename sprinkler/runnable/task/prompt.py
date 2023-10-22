from __future__ import annotations

from typing import Any, List, Dict

from sprinkler.runnable.task import Task
from sprinkler.operation import construct_messages


class PromptTask(Task):
    """Task Class for constructing templates"""
    def __init__(
        self, 
        id_: str,
        context_: Dict[str | Any] | None = None,
        *,
        input_config: Dict[str, Dict] = {}
    ) -> None:
        
        if 'messages' in input_config:
            input_config['messages']['type'] = List[Any]
        else:
            input_config['messages'] = {'type': List[Any]}

        if 'input_variables' in input_config:
            input_config['input_variables']['type'] = Dict[str, Any]
        else:
            input_config['input_variables'] = {'type': Dict[str, Any]}

        super().__init__(id_, 
                        construct_messages,
                        context_,
                        input_config=input_config,
                        output_config=List[Dict [str, str]])