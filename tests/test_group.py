from sprinkler import Pipeline, Group, Task, Ctx


def test_group_with_context():
    @Task('repeat string')
    def repeat_string(string: str, repeat: Ctx[int]) -> str:
        return string * repeat
    
    @Task('repeat array')
    def repeat_array(array: list, repeat: Ctx[int]) -> list:
        return array * repeat

    group = Group('group').add(
        Pipeline('pipeline1').add(
            repeat_string
        ),
        Pipeline('pipeline2').add(
            repeat_array
        )
    )

    output = group.run_with_context(
        {'repeat': 3},
        pipeline1='sprinkler',
        pipeline2=[1,2,3]
    )

    assert output == {
        'pipeline1': 'sprinklersprinklersprinkler',
        'pipeline2': [1, 2, 3, 1, 2, 3, 1, 2, 3]
    }


def test_group_with_tuple_input():
    @Task('repeat string')
    def repeat_string(string: str, repeat: Ctx[int]) -> str:
        return string * repeat
    
    @Task('add')
    def add(a: int, b: int) -> int:
        return a + b

    group = Group('group').add(
        Pipeline('p1').add(
            repeat_string
        ),
        Pipeline('p2').add(
            add
        )
    )

    output = group.run_with_context(
        {'repeat': 3},
        p2=(3,4),
        __default__='sprinkler'
    )

    assert output == {
        'p1': 'sprinklersprinklersprinkler',
        'p2': 7
    }


def test_group_with_no_input():
    @Task('repeat string')
    def repeat_string(string: str, repeat: Ctx[int]) -> str:
        return string * repeat
    
    @Task('return string')
    def return_string() -> str:
        return 'sprinkler'

    group = Group('group').add(
        Pipeline('p1').add(repeat_string),
        Pipeline('p2').add(return_string)
    )

    output = group.run_with_context(
        {'repeat': 3},
        p1='sprinkler'
    )

    assert output == {
        'p1': 'sprinklersprinklersprinkler',
        'p2': 'sprinkler'
    }


def test_group_with_default_arguments():
    @Task('repeat string')
    def repeat_string(string: str, repeat: int = 3) -> str:
        return string * repeat
    
    @Task('repeat array')
    def repeat_array(array: list, repeat: int = 3) -> list:
        return array * repeat

    group = Group('group', max_workers=2).add(
        Pipeline('p1').add(repeat_string),
        Pipeline('p2').add(repeat_array)
    )

    output = group.run(
        p1=('sprinkler',),
        p2=([1,2,3],)
    )

    assert output == {
        'p1': 'sprinklersprinklersprinkler',
        'p2': [1, 2, 3, 1, 2, 3, 1, 2, 3]
    }


def repeat_string(string: str, repeat: int = 3) -> str:
    return string * repeat
def repeat_array(array: list, repeat: int = 3) -> list:
    return array * repeat

def test_group_with_processpool():

    pipeline1 = Pipeline('pipeline1')
    pipeline1.add(Task('repeat_string', repeat_string))

    pipeline2 = Pipeline('pipeline2')
    pipeline2.add(Task('repeat_array', repeat_array))

    group = Group('group', executor_type='process', max_workers=2).add(
        pipeline1, pipeline2
    )

    output = group.run(
        pipeline1=('sprinkler',),
        pipeline2=([1,2,3],)
    )

    assert output == {
        'pipeline1': 'sprinklersprinklersprinkler',
        'pipeline2': [1, 2, 3, 1, 2, 3, 1, 2, 3]
    }