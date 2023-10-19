import pytest

from sprinkler.runnable.task.chat import ChatCompletionTask

def test_chat_completion_task_base():
    task = ChatCompletionTask(
        'chat#1'
    )

    chat_response = task(
        [{'role': 'user', 'content': 'Hello, my name is Jung Sik'}])
    
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