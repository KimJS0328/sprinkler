import pytest

from sprinkler.runnable.task import Task
from sprinkler.runnable.task.chat import ChatCompletionTask

def test_chat_completion_task_base():
    task = ChatCompletionTask(
        'chat#1'
    )

    chat_response = task(
        [{'role': 'user', 'content': 'Hello, my name is Jung Sik'}])
    
    assert chat_response is not None

