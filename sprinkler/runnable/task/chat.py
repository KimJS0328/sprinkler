from __future__ import annotations

import os
from typing import Any, Dict

from sprinkler.runnable.task import Task
from sprinkler.operations import chat_completion, achat_completion


class ChatCompletionTask(Task):
    """Task Class for chat completion with LLM"""
    def __init__(
        self, 
        id_: str, 
        *,
        context: Dict[str | Any] | None = None,
        async_mode: bool = False
    ) -> None:
        if not ('OPENAI_API_KEY' in os.environ):
            raise Exception('No OpenAI API key provided')
        
        if not async_mode:
            super().__init__(id_, 
                            chat_completion,
                            context=context)
        else:
            super().__init__(id_, 
                            achat_completion,
                            context=context)