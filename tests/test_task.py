from __future__ import annotations

from typing import List, Dict, Union, Tuple

import pytest

from sprinkler import Task, Ann, Ctx


class A:
    a: int
    b: str
    def __init__(self, a, b) -> None:
        self.a = a
        self.b = b
    def __eq__(self, __value: object) -> bool:
        return (
            self.a == __value.a and self.b == __value.b
        )

class B:
    a: A
    b: tuple
    def __init__(self, a, b) -> None:
        self.a = a
        self.b = b
    def __eq__(self, __value: object) -> bool:
        return (
            self.a == __value.a and self.b == __value.b
        )


def test_task():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'task1',
        operation
    )

    output = task.run('sprinkler', 3)

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_list_type():
    def filter_odd(arr: List[int]) -> List[int]:
        return [i for i in arr if i % 2 == 1]

    task = Task(
        'task1',
        filter_odd
    )

    output = task.run([1,2,3,4,5])

    assert output == [1,3,5]


def test_task_pydantic_coersion():
    def map_value(a: Dict[str, str]) -> Dict[str, int]:
        return {key: val for key, val in a.items()}

    task = Task(
        'task1',
        map_value
    )

    output = task.run({'a': '1'})

    assert output == {'a': 1}


def test_task_with_kwargs():
    def operation(b: int, a: str) -> str:
        return a * b

    task = Task(
        'task1',
        operation
    )

    output = task(b=3, a='sprinkler')

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_default_arguments():
    def operation(a: str, b: int = 3) -> str:
        return a * b

    task = Task(
        'task1',
        operation
    )

    output = task.run('sprinkler')

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_no_type_hint():
    def operation(a, b):
        return a * b

    task = Task(
        'task1',
        operation
    )

    output = task.run('sprinkler', 3)

    assert output == 'sprinklersprinklersprinkler'


def test_task_with_complex_type_hint():
    
    
    @Task('task1')
    def op(a: Ann[B], b: Union[A, Dict[str, Tuple[int,int]]]) -> Dict[str, B]:
        return {
            'test': B(A(3,'5'), (2,5))
        }
    
    output = op.run(B(A(1,'1'), (5,2)), {'a': (3,5)})
    
    assert output == {
        'test': B(A(3,'5'), (2,5))
    }


def test_task_type_error_with_complex_type_hint():

    @Task('task1')
    def op(a: Ann[B], b: Union[A, Dict[str, Tuple[int,int]]]) -> Dict[str, B]:
        return {
            'test': B(A(3,'5'), (2,5))
        }
    
    with pytest.raises(Exception) as err:
        output = op.run(B(A(1,'1'), (5,2)), {'a': (3,)})
    
    assert 'input' in err.value.args[0]


def test_task_type_error_with_none_input():
    def operation(a: str, b: int = 3) -> str:
        return a * b

    task = Task(
        'task1',
        operation
    )

    with pytest.raises(Exception) as err:
        output = task.run(a=None)

    assert 'input' in err.value.args[0]


def test_input_name_error():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'task1',
        operation
    )

    with pytest.raises(Exception) as err:
        output = task.run('sprinkler', c=3)

    assert 'input' in err.value.args[0]


def test_input_type_error():
    def operation(a: str, b: int) -> str:
        return a * b

    task = Task(
        'task1',
        operation
    )

    with pytest.raises(Exception) as err:
        output = task(a='sprinkler', b='hello')
    
    assert 'input' in err.value.args[0]


def test_output_type_error():
    def operation(a: str, b: int) -> None:
        return a * b

    task = Task(
        'task1',
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


def test_task_without_paramter():
    def operation():
        return 5
    
    task = Task(
        'without parameter',
        operation
    )

    output = task()

    assert output == 5