from typing import List

from sprinkler import Pipeline, Task


def test_task_decorator():
    @Task('add')
    def add(a: int, b: int) -> int:
        return a + b
    
    task = add
    output = task(5, 5)

    assert output == 10


def test_task_decorator_with_pipeline():
    @Task('task1')
    def filter_odd(arr: List[int]) -> List[int]:
        return [i for i in arr if i % 2 == 1]

    @Task('task2')
    def map_twice(arr: List[int]) -> List[int]:
        return [i*2 for i in arr]
    
    pipeline = Pipeline('pipeline')
    pipeline.add(filter_odd)
    pipeline.add(map_twice)

    assert [2,6,10] == pipeline.run([1,2,3,4,5])