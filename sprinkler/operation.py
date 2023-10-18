from __future__ import annotations

from typing import Any, List, Dict, Union

import openai

from sprinkler import config
from sprinkler.prompt_template import PromptTemplate


def construct_messages(
    messages: List[Union[str, PromptTemplate]],
    input_variables: Dict[str, Any]
) -> List[Dict [str, str]]:
    for i, message in enumerate(messages):
        if isinstance(message, str):
            messages[i] = PromptTemplate(message).get_message()
        elif issubclass(message.__class__, PromptTemplate):
            messages[i] = message.get_message(**input_variables)
        else:
            raise TypeError(f'Message must be string or instance of PromptTemplate')
        
    return messages



    
    