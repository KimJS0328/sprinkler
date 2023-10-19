from __future__ import annotations

from typing import Any, List, Dict, Union
import json

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


def chat_completion(
    messages: List[Dict[str, Any]],
    whole_output: bool = False,
    *,
    model: str = config.DEFAULT_OPENAI_MODEL,
    retry_count: int = 1,
    frequency_penalty: float = None,
    function_call: Union[str, dict] = None,
    functions: List[Dict] = None,
    logit_bias: Dict[int, float] = None,
    max_tokens: int = None,
    n: int = None,
    presence_penalty = None,
    stop: Union[str, List] = None,
    temperature: float = None,
    top_p: float= None,
    user: str = None 
) -> Union[Dict, List, str]:

    kwargs = {
        'frequency_penalty': frequency_penalty,
        'function_call': function_call,
        'functions': functions,
        'logit_bias': logit_bias,
        'max_tokens': max_tokens,
        'n': n,
        'presence_penalty': presence_penalty,
        'stop': stop,
        'temperature': temperature,
        'top_p': top_p,
        'user': user
    }   

    for _ in range(retry_count):  
        try:
            response = openai.ChatCompletion.create(
                model = model,
                messages = messages,
                **{k: v for k, v in kwargs.items() if v is not None}
            )

        except Exception as e:
            print(f'API Error: {e}')
            continue

        if whole_output:
            return json.loads(str(response))

        if n is None:
            n = 1

        msg_key = 'content' if functions is None else 'function_call'
        output = [response['choices'][i]['message'][msg_key] for i in range(n)]
            
        if n == 1:
            return output[0]
        else:
            return output
    
    