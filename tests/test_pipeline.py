
from sprinkler.task import Task
from sprinkler.pipeline import Pipeline


def test_pipeline_base():
    def operation1(a: int, b: int) -> int:
        return a ** b
    
    def operation2(a: int, b: int) -> int:
        return a + b
    
    task1 = Task(
        'task1',
        operation1,
        {'a': 3, 'b': 4}
    )

    task2 = Task(
        'task2',
        operation2,
        context = {'b': 5},
        input_config={'a': {'type': int, 'src': 'return'}}
    )

    pipeline = Pipeline()
    pipeline.add_task(task1)
    pipeline.add_task(task2)
    output = pipeline.run_sequential()

    assert output == 86