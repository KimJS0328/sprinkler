from sprinkler.task import Task
from sprinkler.pipeline import Pipeline, Group


def test_group_with_context():
    def repeat_string(string: str, repeat: int) -> str:
        return string * repeat
    
    def repeat_array(array: list, repeat: int) -> list:
        return array * repeat
    
    pipeline1 = Pipeline('pipeline1')
    pipeline1.add_task(Task('repeat_string', repeat_string))

    pipeline2 = Pipeline('pipeline2')
    pipeline2.add_task(Task('repeat_array', repeat_array))

    group = Group('group')
    group.add_pipeline(pipeline1)
    group.add_pipeline(pipeline2)

    result = group.run_with_context(
        {'repeat': 3},
        pipeline1='sprinkler',
        pipeline2=[1,2,3]
    )

    assert result == {
        'pipeline1': 'sprinklersprinklersprinkler',
        'pipeline2': [1, 2, 3, 1, 2, 3, 1, 2, 3]
    }