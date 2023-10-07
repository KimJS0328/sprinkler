
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

    pipeline = Pipeline()
    pipeline.add_task(task1)
    pipeline.add_task(task2)
    output = pipeline.run(3, 2)

    assert output == 729