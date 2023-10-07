import pytest
from pydantic import ValidationError

from sprinkler.task import Task
from sprinkler.context import Context
from sprinkler import config


def test_task_base():
    def operation(a, b) -> str:
        return a * b

    task = Task(
        'test_task_base',
        operation,
        {'a': str, 'b': int}
    )

    output = task.run('sprinkler', 3)

    assert output == 'sprinklersprinklersprinkler'


def test_parse_default_output():
    def operation(b, a: str = 'hello') -> str:
        return a * b

    task = Task(
        'test_parse_default_output',
        operation,
        {'b': int}
    )

    output = task(a='sprinkler', b=3)

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_default():
    def operation(a: str, b: int = 3) -> str:
        return a * b

    task = Task(
        'test_task_with_default',
        operation
    )

    output = task.run(a='sprinkler')

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_no_type_hint():
    def operation(a: str, b):
        return a * b

    task = Task(
        'test_task_with_no_type_hint',
        operation,
        output_config=str
    )

    output = task.run('sprinkler', 3)

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_input_config():
    def operation(a: str, b: str) -> str:
        return a * b

    task = Task(
        'test_task_with_input_config',
        operation,
        {'b': int}
    )

    output = task.run('sprinkler', 3)

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_output_config():
    def operation(a: str, b: int) -> None:
        return a * b

    task = Task(
        'test_task_with_output_config',
        operation,
        output_config=str
    )

    output = task.run('sprinkler', 3)

    assert output == 'sprinklersprinklersprinkler'


def test_type_error_with_none_input():
    def operation(a: str, b: int = 3) -> str:
        return a * b

    task = Task(
        'test_type_error_with_none_input',
        operation,
        {'a': str}
    )

    with pytest.raises(Exception) as err:
        output = task.run(a=None)

    assert 'input' in err.value.args[0]


def test_input_name_error():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'test_input_name_error',
        operation
    )

    with pytest.raises(Exception) as err:
        output = task.run('sprinkler', c=3)

    assert 'unexpected' in err.value.args[0]


def test_input_type_error():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'test_input_type_error',
        operation
    )

    with pytest.raises(Exception) as err:
        output = task(a='sprinkler', b='hello')
    
    assert 'input' in err.value.args[0]


def test_output_type_error():
    def operation(a: str, b: int) -> None:
        return a * b

    task = Task(
        'test_output_type_error',
        operation
    )

    with pytest.raises(Exception) as err:
        output = task.run('sprinkler', 3)
    
    assert 'output' in err.value.args[0]
    

def test_instance_method():
    class Test:
        def run(self, a: int):
            return a * a

    t = Test()
    task = Task(
        'test_with_instance',
        t.run
    )

    output = task(5)

    assert output == 25