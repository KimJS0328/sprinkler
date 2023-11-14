

from typing import Any, List, Dict, Union
import json

import openai

from sprinkler import constants
from sprinkler.runnable.task.base import Ann, Ctx
from sprinkler.prompt_template import PromptTemplate


def construct_messages(
    messages: Ctx[List[Union[str, Dict, PromptTemplate]]],
    input_variables: Ann[Dict[str, Any]]
) -> List[Dict [str, str]]:
    """Construct unformatted to formatted messages as input
    
    Attributes:
        messages: unformatted typed messages
        input_variables: mapping for variables in format 
        string in promptTemplate
    """
    for i, message in enumerate(messages):
        if isinstance(message, str):
            messages[i] = PromptTemplate(message).get_message()
        
        elif isinstance(message, dict):
            continue

        elif issubclass(message.__class__, PromptTemplate):
            messages[i] = message.get_message(**input_variables)

        else:
            raise TypeError(f'Message must be string or instance of PromptTemplate')
        
    return messages


def chat_completion(
    messages: Ann[List[Dict[str, Any]]],
    whole_output: Ctx[bool] = False,
    *,
    model: Ctx[str] = constants.DEFAULT_OPENAI_MODEL,
    retry_count: Ctx[int] = 1,
    frequency_penalty: Ctx[float] = None,
    function_call: Ctx[Union[str, dict]] = None,
    functions: Ctx[List[Dict]] = None,
    logit_bias: Ctx[Dict[int, float]] = None,
    max_tokens: Ctx[int] = None,
    n: Ctx[int] = None,
    presence_penalty: Ctx[float] = None,
    stop: Ctx[Union[str, List]] = None,
    temperature: Ctx[float] = None,
    top_p: Ctx[float]= None,
    user: Ctx[str] = None 
) -> Union[Dict, List, str]:
    """OpenAI ChatGPT chat completion request
    
    Attributes:
        whole_output: if True, return whole output of response(dictonary),
        it not, return only content (function call if functions exsists)
        retry_count: the number of retries if error exists API request,
        default is 1.

        other attributes is from 
        https://platform.openai.com/docs/api-reference/chat/create
    """

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
    
    