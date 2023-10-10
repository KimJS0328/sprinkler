from typing import Tuple

from sprinkler import Pipeline, Group
from sprinkler.decorator import Task


def test_pipeline_of_group():

    p = Pipeline('p')
    a = Group('a')
    b = Group('b')

    @Task('t1')
    def t1(a: int, b: int) -> int:
        return a + b
    
    @Task('t2')
    def t2(a: str, b: str) -> str:
        return a + b
    
    @Task('t3')
    def t3(t2: str, t1: int) -> str:
        return t2 * t1
    
    @Task('t4')
    def t4(t2: str, t1: int) -> str:
        return f'{t2}-{t1}'
    
    a.add(t1)
    a.add(t2)

    b.add(t3)
    b.add(t4)

    p.add(a)
    p.add(b)

    output = p.run(t1=(1,2), t2=('hello', 'world'))

    assert output == {
        't3': 'helloworldhelloworldhelloworld',
        't4': 'helloworld-3'
    }


def test_pipeline_of_group2():

    p = Pipeline('p')
    a = Group('a')
    b = Group('b')

    @Task('t1')
    def t1(a: int, b: int) -> Tuple[int,int]:
        return a * 2, b * 2
    
    @Task('t2')
    def t2(a: str, b: str) -> str:
        return a + b
    
    @Task('t3', input_config={'a': {'src': 'a.t2'}, 'b': {'src': 'a.t1'}})
    def t3(a: str, b: Tuple[int,int]) -> str:
        return a * b[0]
    
    @Task('t4', input_config={'a': {'src': 'a.t2'}, 'b': {'src': 'a.t1'}})
    def t4(a: str, b: Tuple[int,int]) -> str:
        return f'{a}-{b[1]}'
    
    a.add(t1)
    a.add(t2)

    b.add(t3)
    b.add(t4)

    p.add(a)
    p.add(b)

    output = p.run(t1=(1,2), t2=('hello', 'world'))

    assert output == {
        't3': 'helloworldhelloworld',
        't4': 'helloworld-4'
    }

