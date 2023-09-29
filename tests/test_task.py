import pytest
from pydantic import ValidationError

from sprinkler.task import Task
from sprinkler.context import Context


def test_task_base():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'test_task_base',
        operation,
        {'a': 'sprinkler', 'b': 3}
    )
    context = Context()
    task.execute(context)
    output = context.get_values({Task.DEFAULT_OUTPUT_KEY: Task.DEFAULT_OUTPUT_KEY})[Task.DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_parse_default_output():
    def operation(b, a: str = 'hello') -> str:
        return a * b

    task = Task(
        'test_parse_default_output',
        operation,
        {Task.DEFAULT_OUTPUT_KEY: 'sprinkler', 'b': 3}
    )
    context = Context()
    task.execute(context)
    output = context.get_values({Task.DEFAULT_OUTPUT_KEY: Task.DEFAULT_OUTPUT_KEY})[Task.DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_default():
    def operation(a: str, b: int = 3) -> str:
        return a * b

    task = Task(
        'test_task_with_default',
        operation,
        {'a': 'sprinkler'}
    )
    context = Context()
    task.execute(context)
    output = context.get_values({Task.DEFAULT_OUTPUT_KEY: Task.DEFAULT_OUTPUT_KEY})[Task.DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_no_type_hint():
    def operation(a: str, b) -> str:
        return a * b

    task = Task(
        'test_task_with_no_type_hint',
        operation,
        {'a': 'sprinkler', 'b': 3}
    )
    context = Context()
    task.execute(context)
    output = context.get_values({Task.DEFAULT_OUTPUT_KEY: Task.DEFAULT_OUTPUT_KEY})[Task.DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_input_config():
    def operation(a: str, b: str) -> str:
        return a * b

    task = Task(
        'test_task_with_input_config',
        operation,
        {'a': 'sprinkler', 'b': 3},
        {'b': int}
    )
    context = Context()
    task.execute(context)
    output = context.get_values({Task.DEFAULT_OUTPUT_KEY: Task.DEFAULT_OUTPUT_KEY})[Task.DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_output_config():
    def operation(a: str, b: int) -> None:
        return a * b

    task = Task(
        'test_task_with_output_config',
        operation,
        {'a': 'sprinkler', 'b': 3},
        output_config=str
    )
    context = Context()
    task.execute(context)
    output = context.get_values({Task.DEFAULT_OUTPUT_KEY: Task.DEFAULT_OUTPUT_KEY})[Task.DEFAULT_OUTPUT_KEY]

    assert output == 'sprinklersprinklersprinkler'


def test_type_error_with_none_input():
    def operation(a: str, b: int = 3) -> str:
        return a * b

    task = Task(
        'test_type_error_with_none_input',
        operation,
        {'a': None}
    )
    context = Context()
    with pytest.raises(Exception) as err:
        task.execute(context)

    assert 'input' in err.value.args[0]


def test_input_name_error():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'test_input_name_error',
        operation,
        {'a': 'sprinkler', 'c': 3}
    )

    context = Context()
    with pytest.raises(Exception) as err:
        task.execute(context)

    assert 'input' in err.value.args[0]


def test_input_type_error():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'test_input_type_error',
        operation,
        {'a': 'sprinkler', 'b': 'hello'}
    )

    context = Context()
    with pytest.raises(Exception) as err:
        task.execute(context)
    
    assert 'input' in err.value.args[0]


def test_output_type_error():
    def operation(a: str, b: int) -> None:
        return a * b

    task = Task(
        'test_output_type_error',
        operation,
        {'a': 'sprinkler', 'c': 3}
    )

    context = Context()
    with pytest.raises(Exception) as err:
        task.execute(context)
    
    assert 'output' in err.value.args[0]