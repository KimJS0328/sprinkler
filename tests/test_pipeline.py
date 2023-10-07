
from sprinkler.task import Task
from sprinkler.pipeline import Pipeline


def test_pipeline_base():
    def operation1(a: int, b: int) -> int:
        return a ** b

    def operation2(a: int) -> int:
        return a ** 3

    task1 = Task(
        'task1',
        operation1
    )

    task2 = Task(
        'task2',
        operation2
    )

    p = Pipeline('test_pipeline_base')
    p.add_task(task1)
    p.add_task(task2)
    output = p.run(3, 2)

    assert output == 729


def test_pipeline_with_args():
    def operation1(a, b):
        return (a * 3, b * 3)

    def operation2(a: int, b: int) -> int:
        return a * b

    task1 = Task(
        'task1', 
        operation1, 
        {'a': int, 'b': int},
        tuple
    )

    task2 = Task(
        'task2',
        operation2
    )

    p = Pipeline('test_pipeline_with_args')
    p.add_task(task1)
    p.add_task(task2)
    output = p.run(5, 6)

    assert output == 270


def test_pipeline_with_history():
    def operation1(a, b):
        return a * b

    def operation2():
        pass

    def operation3(a):
        return a + 5

    task1 = Task(
        'task1',
        operation1
    )

    task2 = Task(
        'task2',
        operation2,
        output_config=None
    )

    task3 = Task(
        'task3',
        operation3,
        {'a': {'src': 'task1'}}
    )

    p = Pipeline('test_pipeline_with_history')
    p.add_task(task1)
    p.add_task(task2)
    p.add_task(task3)

    output = p.run(2, 10)

    assert output == 25


def test_pipeline_with_context():
    def operation1(a, b):
        return a * b

    def operation2():
        pass

    def operation3(a):
        return a + 5

    task1 = Task(
        'task1',
        operation1
    )

    task2 = Task(
        'task2',
        operation2,
        output_config=None
    )

    task3 = Task(
        'task3',
        operation3,
        {'a': {'src': 'task1'}}
    )

    p = Pipeline('test_pipeline_with_context', {'b': 10})
    p.add_task(task1)
    p.add_task(task2)
    p.add_task(task3)

    output = p.run(2)

    assert output == 25