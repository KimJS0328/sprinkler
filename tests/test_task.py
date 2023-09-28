import pytest

from sprinkler.task import Task
from sprinkler.context import Context


def test_task_1():
    task = Task(
        'test_task_1',
        {'input0': 'str', 'input1': 'int'},
        'str',
        {'input0': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )
    context = Context()
    task.execute(context)
    output = context.retrieve({Task._DEFAULT_OUTPUT_KEY: ''})[Task._DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_input_name_error():
    task = Task(
        'test_input_name_error',
        {'input0': 'str', 'input1': 'int'},
        'str',
        {'input2': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )

    context = Context()
    with pytest.raises(NameError) as err:
        task.execute(context)
    assert 'The input' in err.value.args[0]


def test_input_type_error():
    task = Task(
        'test_input_type_error',
        {'input0': 'str', 'input1': 'str'},
        'str',
        {'input0': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )

    context = Context()
    with pytest.raises(TypeError) as err:
        task.execute(context)
    assert 'The input' in err.value.args[0]


def test_output_type_error():
    task = Task(
        'test_output_type_error',
        {'input0': 'str', 'input1': 'int'},
        'int',
        {'input0': 'sprinkler', 'input1': 3},
        lambda input0, input1: input0 * input1
    )

    context = Context()
    with pytest.raises(TypeError) as err:
        task.execute(context)
    assert 'The output' in err.value.args[0]