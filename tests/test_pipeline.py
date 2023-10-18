from sprinkler import Task, Pipeline


def test_pipeline():
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

    p = Pipeline('pipeline')
    p.add(task1)
    p.add(task2)
    output = p.run(3, 2)

    assert output == 729


def test_pipeline_with_args():
    def operation1(a, b):
        return a * 3, b * 3

    def operation2(a: int, b: int) -> int:
        return a * b

    task1 = Task(
        'task1', 
        operation1, 
        input_config={'a': int, 'b': int},
        output_config=tuple
    )

    task2 = Task(
        'task2',
        operation2
    )

    p = Pipeline('pipeline')
    p.add(task1)
    p.add(task2)
    output = p.run(5, 6)

    assert output == 270


def test_pipeline_with_default_args():
    def operation1(a, b = 6):
        return a * 3, b * 3

    def operation2(a: int, b: int) -> int:
        return a * b

    task1 = Task(
        'task1', 
        operation1, 
        input_config={'a': int, 'b': int},
        output_config=tuple
    )

    task2 = Task(
        'task2',
        operation2
    )

    p = Pipeline('pipeline')
    p.add(task1)
    p.add(task2)
    output = p.run(5)

    assert output == 270


def test_pipeline_with_default_args2():
    def operation1(a, b, c = 6):
        return a * 3, b * 3, c * 3

    def operation2(a: int, b: int) -> int:
        return a * b

    task1 = Task(
        'task1', 
        operation1, 
        input_config={'a': int, 'b': int},
        output_config=tuple
    )

    task2 = Task(
        'task2',
        operation2
    )

    p = Pipeline('pipeline')
    p.add(task1)
    p.add(task2)
    output = p.run(5, b = 6)

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
        input_config={'a': {'src': 'task1'}}
    )

    p = Pipeline('pipeline')
    p.add(task1)
    p.add(task2)
    p.add(task3)

    output = p.run(2, 10)

    assert output == 25


def test_pipeline_with_context1():
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
        input_config={'a': {'src': 'task1'}}
    )

    p = Pipeline('pipeline', context={'b': 10})
    p.add(task1)
    p.add(task2)
    p.add(task3)

    output = p.run(2)

    assert output == 25


def test_pipeline_with_context2():
    def operation1(a, b):
        return a * b

    def operation2():
        pass

    def operation3(a, b):
        return a + 5 * b

    task1 = Task(
        'task1',
        operation1
    )

    task2 = Task(
        'task2',
        operation2
    )

    task3 = Task(
        'task3',
        operation3,
        input_config={'a': {'src': 'task1'}}
    )

    p = Pipeline('pipeline', context={'b': 10})
    p.add(task1)
    p.add(task2)
    p.add(task3)

    output = p.run(2)

    assert output == 70