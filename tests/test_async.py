import asyncio

import pytest

from sprinkler.decorator import Task

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