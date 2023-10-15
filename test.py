import asyncio
from concurrent.futures import ThreadPoolExecutor, Future

async def async_func():
    # await asyncio.sleep(1)
    return 'hello'

def hehe(coro):
    return asyncio.run(coro)

def sync_func():
    with ThreadPoolExecutor() as executor:
        future = executor.submit(hehe, async_func())
        return (future.result())


async def main():
    return sync_func()

print(asyncio.run(main()))