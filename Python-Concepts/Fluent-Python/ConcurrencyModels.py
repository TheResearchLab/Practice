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
# import asyncio 
# import itertools
# import time

# async def spin(msg: str) -> None:
#     for char in itertools.cycle(r'\|/-'):
#         status = f'\r{char} {msg}'
#         print(status,flush=True,end='')
#         try:
#             await asyncio.sleep(.1) # not quite understanding this
#         except asyncio.CancelledError:
#             break 
    
#     blanks = ' ' * len(status)
#     print(f'\r{blanks}\r', end='')

# async def slow() -> int:
#     await asyncio.sleep(3) # time.sleep here would block the GIL from allowing the spinner to run
#     return 42


# async def supervisor() -> int:
#     spinner = asyncio.create_task(spin('thinking!')) # returns an asyncio.Task
#     print(f'spinner object: {spinner}')
#     result = await slow()
#     spinner.cancel()
#     return result 

# def main() -> None:
#     result = asyncio.run(supervisor())
#     print(f'Answer: {result}')


# if __name__ == '__main__':
#     main()

""" GIL Impact Example"""
# import math

# def is_prime(n:int) -> bool:
#     if n<2:
#         return False
#     if n==2:
#         return True 
#     if n%2 == 0:
#         return False
    
#     root = math.isqrt(n)
#     for i in range(3, root+1,2):
#         if n % 1 == 0:
#             return False
#     return True 

# from time import perf_counter
# from typing import NamedTuple

# NUMBERS = [2,142702110479723,299593572317531,3333335652092209,9999999999999917,5555553133149889]

# class Result(NamedTuple):
#     prime: bool
#     elapsed: float

# def check(n:int) -> Result:
#     t0 = perf_counter()
#     prime = is_prime(n)
#     return Result(prime,perf_counter() - t0)

# def main() -> None:
#     print(f'Checking {len(NUMBERS)} numbers sequentially:')
#     t0 = perf_counter()
#     for n in NUMBERS:
#         prime, elapsed = check(n)
#         label = 'P' if prime else ' '
#         print(f'{n:6} {label} {elapsed:9.6f}s')

#     elapsed = perf_counter() - t0 
#     print(f'Total time: {elapsed:.2f}s')

# if __name__ == '__main__':
#     main()

""" Multicore Prime Checker """
import sys 
from time import perf_counter
from typing import NamedTuple
from multiprocessing import Process, SimpleQueue, cpu_count # cant use simple queue in type hints
from multiprocessing import queues 

import math

def is_prime(n:int) -> bool:
    if n<2:
        return False
    if n==2:
        return True 
    if n%2 == 0:
        return False
    
    root = math.isqrt(n)
    for i in range(3, root+1,2):
        if n % 1 == 0:
            return False
    return True 

NUMBERS = [2,142702110479723,299593572317531,3333335652092209,9999999999999917,5555553133149889]

class PrimeResult(NamedTuple):
    n: int 
    prime: bool 
    elapsed: float 

JobQueue = queues.SimpleQueue[int]
ResultQueue = queues.SimpleQueue[PrimeResult]

def check(n:int) -> PrimeResult:
    t0 = perf_counter()
    res = is_prime(n)
    return PrimeResult(n, res, perf_counter() - t0)

def worker(jobs: JobQueue, results: ResultQueue) -> None:
    while n:= jobs.get():
        results.put(check(n))
    results.put(PrimeResult(0,False,0.0))

def start_jobs(
        procs: int, jobs: JobQueue, results ResultQueue
) -> None:
    for n in NUMBERS:
        jobs.put(n)
    for _ in range(procs): 
        proc = Process(target=worker, args=(jobs,results))
        proc.start()
        jobs.put(0)

def main() -> None:
    if len(sys.argv) < 2:
        procs = cpu_count()
    else:
        procs = int(sys.argv[1])

    print(f'Checking {len(NUMBERS)} numbers with {procs} processes:')
    t0 = perf_counter()
    jobs: JobQueue = SimpleQueue()
    results: ResultQueue = SimpleQueue()
    start_jobs(procs, jobs, results)
    checked = report(procs, results)
    elapsed = perf_counter() - t0 
    print(f'{checked} checks in {elapsed:.2f}s')

def report(procs: int, results: ResultQueue) -> int: 
    checked = 0
    procs_done = 0 
    while procs_done < procs:
        n, prime, elapsed = results.get()
        if n == 0:
            procs_done += 1
        else:
            checked +=1 
            label = 'P' if prime else ' '
            print(f'{n:16} {label} {elapsed:9.6f}s')
    return checked 

if __name__ == '__main__':
    main()
