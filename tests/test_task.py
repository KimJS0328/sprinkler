import pytest

from sprinkler.task import Task
from sprinkler.context import Context


def test_task_1():
    task = Task(
        {'input0': 'str', 'input1': 'int'},
        'str',
        {'input0': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )
    context = Context()
    task.execute(context)
    output = context.retrieve({Task._DEFAULT_OUTPUT_KEY: ''})[Task._DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_value_error():
    task = Task(
        {'input0': 'str', 'input1': 'int'},
        'str',
        {'input2': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )

    context = Context()
    with pytest.raises(ValueError):
        task.execute(context)


def test_type_error():
    task = Task(
        {'input0': 'str', 'input1': 'str'},
        'str',
        {'input0': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )

    context = Context()
    with pytest.raises(TypeError):
        task.execute(context)