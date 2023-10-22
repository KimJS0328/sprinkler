from __future__ import annotations

import os
from typing import Any, List, Dict

import openai

from sprinkler.runnable.task import Task
from sprinkler.operation import chat_completion


class ChatCompletionTask(Task):
    """Task Class for chat completion with LLM"""
    def __init__(
        self, 
        id_: str, 
        context_: Dict[str | Any] | None = None,
        *,
        input_config: Dict[str, Dict] = {}
    ) -> None:
        if not ('OPENAI_API_KEY' in os.environ):
            raise Exception('No OpenAI API key provided')

        if ('messages' in input_config):
            input_config['messages']['type'] = List[Dict[str, Any]]
        else:
            input_config['messages'] = {'type': List[Dict[str, Any]]}
        
        super().__init__(id_, 
                        chat_completion,
                        context_,
                        input_config=input_config,
                        output_config=str
                        )