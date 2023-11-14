import pytest

from sprinkler.prompt_template import PromptTemplate, SystemPromptTemplate, AssistantPromptTemplate
from sprinkler.runnable.task import PromptTask
from sprinkler.runnable.task import ChatCompletionTask
from sprinkler import Pipeline

def test_chat_completion_task_base():
    task = ChatCompletionTask(
        'chat#1'
    )

    chat_response = task(
        [{'role': 'user', 'content': 'Hello, my name is Jung Sik'}]
    )
    
    assert chat_response is not None


def test_cc_with_another_paramters():
    task = ChatCompletionTask(
        'chat#2'
    )

    chat_response = task(
        [{'role': 'user', 'content': 'Hello, my name is Jung Sik'}],
        True,
        max_tokens=10,
        temperature=0.5)
    
    assert chat_response['usage']['completion_tokens'] == 10


def test_function_call_base():
    task = ChatCompletionTask(
        'function'
    )

    chat_response = task(
        [{"role": "user", "content": "What is the weather like in Boston?"}],
        functions=[{
            "name": "get_current_weather",
            "description": "Get the current weather in a given location",
            "parameters": {
                    "type": "object",
                    "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city and state, e.g. San Francisco, CA"
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"]
                    }
                },
                "required": ["location"]
            }
        }],
        function_call="auto"
    )

    assert 'arguments' in chat_response


def test_cc_with_multiple_choices():
    task = ChatCompletionTask('multiple')

    chat_response = task(
        [{'role': 'user', 'content': 'Do you agree that Jungsik is genius?'}],
        n = 3
    )

    assert len(chat_response) == 3


def test_pipeline_prompt_chat():
    messages = [SystemPromptTemplate('You are a student under professor Jungsik'),
                AssistantPromptTemplate('Jungsik is {identity}'),
                PromptTemplate('Do you know Jungsik?')]

    task_prompt = PromptTask('prompt',
                      {'messages': messages})
    
    task_chat = ChatCompletionTask('chat',
                                   {'temperature': 0.8,
                                    'retry_count': 2})
    
    pipeline = Pipeline('pipe')
    pipeline.add(task_prompt)
    pipeline.add(task_chat)

    output = pipeline.run({'identity': 'genius'})

    print(output)

    assert 'Jungsik' in output


def test_pipeline_prompt_chat2():
    messages = [SystemPromptTemplate('You are a fan of baseball team named {team}'),
                PromptTemplate('Have you bought {team} merchandise?')]

    task_prompt = PromptTask('prompt',{'messages': messages})
    task_chat = ChatCompletionTask('chat')
    
    pipeline = Pipeline('pipeline').add(task_prompt, task_chat)

    output = pipeline.run({'team': 'sprinkler'})

    print(output)

    assert 'sprinkler' in output