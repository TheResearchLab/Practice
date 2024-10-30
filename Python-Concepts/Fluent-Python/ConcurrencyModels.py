""" Threading Example"""

# import itertools 
# import time
# from threading import Thread, Event

# def spin(msg: str, done:Event) -> None:
#     for char in itertools.cycle(r'\|/-'):
#         status = f'\r{char} {msg}'
#         print(status,end='',flush=True)
#         if done.wait(.1):
#             break 
    
#     blanks = ' ' * len(status)
#     print(f'\r{blanks}\r',end='')

# def slow() -> int:
#     time.sleep(3)
#     return 42

# def supervisor() -> int:
#     done = Event() # this will manage flow between threads
#     spinner = Thread(target=spin, args=('thinking!',done)) 
#     print(f'spinner object: {spinner}') # return object in state before starting
#     spinner.start()
#     result = slow() # blocks the main thread and secondary thread runs
#     done.set() # this part kills the infinite loop in spin
#     spinner.join() # waiting for spinner thread to finish
#     return result 

# def main() -> None:
#     result = supervisor()
#     print(f'Answer: {result}')

# if __name__ == "__main__":
#     main() # so cool

""" Multiprocessing Example"""

# import itertools 
# import time 
# from multiprocessing import Process, Event 
# from multiprocessing import synchronize 

# def spin(msg: str, done: synchronize.Event) -> None:
#     for char in itertools.cycle(r'\|/-'):
#         status = f'\r{char} {msg}'
#         print(status,end='',flush=True)
#         if done.wait(.1):
#             break 

# def slow() -> int:
#     time.sleep(3)
#     return 42

# def supervisor() -> int:
#     done = Event()
#     spinner = Process(target=spin,
#                       args=('thinking!',done))
#     print(f'spinner object: {spinner}')
#     spinner.start()
#     result = slow()
#     done.set()
#     spinner.join()
#     return result 

# def main() -> None:
#     result = supervisor()
#     print(f'Answer:{result}')

# if __name__ == '__main__':
#     main()

""" Coroutines Example"""
import asyncio 
import itertools

async def spin(msg: str) -> None:
    for char in itertools.cycle(r'\|/-'):
        status = f'\r{char} {msg}'
        print(status,flush=True,end='')
        try:
            await asyncio.sleep(.1)
        except asyncio.CancelledError:
            break 
    
    blanks = ' ' * len(status)
    print(f'\r{blanks}\r', end='')

async def slow() -> int:
    await asyncio.sleep(3)
    return 42


async def supervisor() -> int:
    spinner = asyncio.create_task(spin('thinking!')) # returns an asyncio.Task
    print(f'spinner object: {spinner}')
    result = await slow()
    spinner.cancel()
    return result 

def main() -> None:
    result = asyncio.run(supervisor())
    print(f'Answer: {result}')


if __name__ == '__main__':
    main()
