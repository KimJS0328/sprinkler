from typing import Tuple

from sprinkler import Pipeline, Group, Task, Ann, K


def test_pipeline_of_group():

    @Task('t1')
    def t1(a: int, b: int) -> int:
        return a + b
    
    @Task('t2')
    def t2(a: str, b: str) -> str:
        return a + b
    
    @Task('t3')
    def t3(a: Ann[str, 't2'], b: Ann[int, 't1']) -> str:
        return a * b
    
    @Task('t4')
    def t4(a: Ann[str, 't2'], b: Ann[int, 't1']) -> str:
        return f'{a}-{b}'

    p = Pipeline('p').add(
        Group('a').add(t1, t2),
        Group('b').add(t3, t4)
    )

    output = p.run(t1=(1,2), t2=('hello', 'world'))

    assert output == {
        't3': 'helloworldhelloworldhelloworld',
        't4': 'helloworld-3'
    }


def test_pipeline_of_group2():
    @Task('t1')
    def t1(a: int, b: int) -> Tuple[int,int]:
        return a * 2, b * 2
    
    @Task('t2')
    def t2(a: str, b: str) -> str:
        return a + b
    
    @Task('t3')
    def t3(a: Ann[str, 't2'], b: Ann[int, K('t1', 0)]) -> str:
        return a * b
    
    @Task('t4')
    def t4(a: Ann[str, 't2'], b: Ann[int, K('t1', 1)]) -> str:
        return f'{a}-{b}'
    
    p = Pipeline('p').add(
        Group('a').add(t1, t2),
        Group('b').add(t3, t4)
    )
    
    output = p.run(t1=(1,2), t2=('hello', 'world'))

    assert output == {
        't3': 'helloworldhelloworld',
        't4': 'helloworld-4'
    }

