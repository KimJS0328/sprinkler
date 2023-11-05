from typing import Tuple
from concurrent.futures import ProcessPoolExecutor

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


def test_nested_group():
    @Task('t1')
    def t1(a: Ann[int, 0], b: Ann[int, 1]) -> Tuple[int,int]:
        return a * 2, b * 2
    
    @Task('t2')
    def t2(a: Ann[str, 2], b: Ann[str, 3]) -> str:
        return a + b
    
    @Task('t3')
    def t3(a: Ann[str, 't2'], b: Ann[int, K('t1', 0)]) -> str:
        return a * b
    
    @Task('t4')
    def t4(a: Ann[str, 't2'], b: Ann[int, K('t1', 1)]) -> str:
        return f'{a}-{b}'
    
    p = Group('g').add(
        Pipeline('p1').add(
            Group('a1').add(t1, t2),
            Group('b1').add(t3, t4)
        ),
        Pipeline('p2').add(
            Group('a2').add(t1, t2),
            Group('b2').add(t3, t4)
        )
    )
    
    output = p.run(p1=(1,2,'hello','world'), p2=(2,3,'hello','world'))

    assert output == {
        'p1': {
            't3': 'helloworldhelloworld',
            't4': 'helloworld-4'
        },
        'p2': {
            't3': 'helloworldhelloworldhelloworldhelloworld',
            't4': 'helloworld-6'
        }
    }



def t1(a: Ann[int, 0], b: Ann[int, 1]) -> Tuple[int,int]:
    return a * 2, b * 2

def t2(a: Ann[str, 2], b: Ann[str, 3]) -> str:
    return a + b

def t3(a: Ann[str, 't2'], b: Ann[int, K('t1', 0)]) -> str:
    return a * b

def t4(a: Ann[str, 't2'], b: Ann[int, K('t1', 1)]) -> str:
    return f'{a}-{b}'

def test_nested_group_with_processpool():

    p = Group('g').add(
        Pipeline('p1').add(
            Group('a1').add(Task('t1', t1), Task('t2', t2)),
            Group('b1').add(Task('t3', t3), Task('t4', t4))
        ),
        Pipeline('p2').add(
            Group('a2').add(Task('t1', t1), Task('t2', t2)),
            Group('b2').add(Task('t3', t3), Task('t4', t4))
        )
    )
    with ProcessPoolExecutor() as executor:
        output = p.run(
            p1=(1,2,'hello','world'),
            p2=(2,3,'hello','world'),
            __executor__=executor
        )

    assert output == {
        'p1': {
            't3': 'helloworldhelloworld',
            't4': 'helloworld-4'
        },
        'p2': {
            't3': 'helloworldhelloworldhelloworldhelloworld',
            't4': 'helloworld-6'
        }
    }