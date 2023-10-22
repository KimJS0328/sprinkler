import asyncio

import pytest

from sprinkler import Pipeline, Group, Task, Ann

def test_async_operation():
    @Task('task1')
    async def task1(a: int) -> str:
        await asyncio.sleep(0)
        return str(a)
    
    assert task1.run(3) == '3'


@pytest.mark.asyncio
async def test_async_run_with_sync_operation():
    @Task('task1')
    def task1(a: int) -> str:
        return str(a)
    
    output = await task1.arun(3)
    assert output == '3'


@pytest.mark.asyncio
async def test_async_run_with_async_operation():
    @Task('task1')
    async def task1(a: int) -> str:
        await asyncio.sleep(0)
        return str(a)
    
    output = await task1.arun(3)
    assert output == '3'


@pytest.mark.asyncio
async def test_async_pipeline_of_group():
    @Task('t1')
    async def t1(a: int, b: int) -> int:
        return a + b
    
    @Task('t2')
    async def t2(a: str, b: str) -> str:
        return a + b
    
    @Task('t3')
    async def t3(a: Ann[str, 't2'], b: Ann[int, t2]) -> str:
        return a * b
    
    @Task('t4')
    async def t4(a: Ann[str, 't2'], b: Ann[int, 't1']) -> str:
        return f'{a}-{b}'
    
    p = Pipeline('p').add(
        Group('a').add(t1, t2),
        Group('b').add(t3, t4)
    )

    output = await p.arun(t1=(1,2), t2=('hello', 'world'))

    assert output == {
        't3': 'helloworldhelloworldhelloworld',
        't4': 'helloworld-3'
    }