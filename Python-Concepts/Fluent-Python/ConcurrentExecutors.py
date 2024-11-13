""" Flags.py example - sequential code """
import time 
from pathlib import Path 
from typing import Callable 

import httpx 

POP2_CC = ('CN IN US ID BR PK NG BD JP '
           'MX PH VN ET EG DE IR TR CD FR').split()

BASE_URL = 'https://www.fluentpython.com/data/flags'
DEST_DIR = Path('downloads')

def save_flag(img:bytes, filename:str) -> None:
    (DEST_DIR / filename).write_bytes(img)

def get_flag(cc: str) -> bytes:
    url = f'{BASE_URL}/{cc}/{cc}.gif'.lower()
    resp = httpx.get(url, timeout=6.1,
                     follow_redirects=True)
    resp.raise_for_status() # raises error if value isn't 2xx
    return resp.content 

def download_many(cc_list: list[str]) -> int:
    for cc in sorted(cc_list):
        image = get_flag(cc)
        save_flag(image, f'{cc}.gif')
        print(cc,end=' ',flush=True)
    return len(cc_list)

def main(downloader: Callable[[list[str]],int]) -> None:
    DEST_DIR.mkdir(exist_ok=True)
    t0 = time.perf_counter()
    count = downloader(POP2_CC)
    elapsed = time.perf_counter() - t0
    print(f'\n{count} downloads in {elapsed:.2f}s')

# if __name__ == '__main__':
#     main(download_many)

""" flags_threadpool.py example - sequential code """
from concurrent import futures 

def download_one(cc:str):
    image = get_flag(cc)
    save_flag(image, f'{cc}.gif')
    print(cc, end=' ',flush=True)
    return cc 

def download_many(cc_list: list[str]) -> int:
    with futures.ThreadPoolExecutor() as executor:
        res = executor.map(download_one, sorted(cc_list))
    
    return len(list(res))

# if __name__ == '__main__':
#     main(download_many)

def download_many(cc_list: list[str]) -> int:
    cc_list = cc_list[:5]
    with futures.ThreadPoolExecutor(max_workers=3) as executor: # changing workers to 1 will cause sequential run
        to_do: list[futures.Future] = []
        for cc in sorted(cc_list):
            future = executor.submit(download_one,cc)
            to_do.append(future)
            print(f'Scheduled for {cc}: {future}')
        
        for count, future in enumerate(futures.as_completed(to_do),1): # as_completed yields futures as the are completed
            res: str = future.result()
            print(f'{future} result: {res!r}')
    
    return count 

# if __name__ == '__main__':
#     main(download_many)

""" Multicore Prime Checker Redux """


import math
import sys 
from time import perf_counter
from typing import NamedTuple 

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

NUMBERS = [9999999999999999,2,142702110479723,299593572317531,3333335652092209,9999999999999917,5555553133149889,6666667141414921,4444444444444444,4444444444444423] 

class PrimeResult(NamedTuple):
    n: int 
    flag: bool 
    elapsed: float 

def check(n: int) -> PrimeResult:
    t0 = perf_counter() 
    res = is_prime(n)
    return PrimeResult(n,res,perf_counter() - t0)

def main() -> None:
    if len(sys.argv) < 2:
        workers = None 
    else:
        workers = int(sys.argv[1])

    executor = futures.ProcessPoolExecutor(workers)
    actual_workers = executor._max_workers 

    print(f'Checking {len(NUMBERS)} numbers with {actual_workers} processes:')
    
    t0 = perf_counter()

    numbers = sorted(NUMBERS,reverse=True)
    with executor:
        for n, prime, elapsed in executor.map(check,numbers):
            label = 'P' if prime else ' '
            print(f'{n:16} {label} {elapsed:9.6f}s')
        
        time = perf_counter() - t0 
        print(f'Total time: {time:.2f}s')
    
# if __name__ ==  '__main__':
#     main()

""" Executor Map """ # Get data back in the same order submitted
from time import sleep, strftime 

def display(*args):
    print(strftime('[%H:%M:%S]'),end=' ')
    print(*args)

def loiter(n):
    msg = '{}loiter({}): doing nothing for {}s...'
    display(msg.format('\t'*n, n, n))
    sleep(n)
    msg = '{}loiter({}): done.'
    display(msg.format('\t'*n,n))
    return n * 10 

def main():
    display('Scripting starting.')
    executor = futures.ThreadPoolExecutor(max_workers=3)
    results = executor.map(loiter,range(5))
    display('result',results)
    display('Waiting for individual results:')
    for i, result in enumerate(results):
        display(f'result {i}: {result}')

if __name__ == '__main__':
    main()


from collections import Counter 
from http import HTTPStatus 
from enum import Enum 


import httpx 
import tqdm # type: ignore

DEFAULT_CONCUR_REQ = 1 
MAX_CONCUR_REQ = 1


DownloadStatus = Enum('DownloadStatus','OK NOT_FOUND ERROR')

def save_flag(img:bytes, filename:str) -> None:
    (DEST_DIR / filename).write_bytes(img)



def main(downloader: Callable[[list[str]],int]) -> None:
    DEST_DIR.mkdir(exist_ok=True)
    t0 = time.perf_counter()
    count = downloader(POP2_CC)
    elapsed = time.perf_counter() - t0
    print(f'\n{count} downloads in {elapsed:.2f}s')

def get_flag(base_url: str, cc: str) -> bytes:
    url = f'{base_url}/{cc}/{cc}.gif'.lower()
    resp = httpx.get(url, timeout=3.1,follow_redirects=True)
    resp.raise_for_status()
    return resp.content 

def download_one(cc:str, base_url: str, verbose: bool = False) -> DownloadStatus:
    try:
        image = get_flag(base_url,cc)
    except httpx.HTTPStatusError as exc:
        res = exc.response 
        if res.status_code == HTTPStatus.NOT_FOUND:
            status = DownloadStatus.NOT_FOUND
            msg = f'not found: {res.url}'
        else:
            raise 
    else:
        save_flag(image,f'{cc}.gif')
        status = DownloadStatus.OK 
        msg = 'OK'
    
    if verbose:
        print(cc,msg)
    
    return status 

def download_many(cc_list: list[str],
                  base_url:str,
                  verbose:bool,
                  _unused_concur_req:int) -> Counter[DownloadStatus]:
    counter: Counter[DownloadStatus] = Counter()
    cc_iter = sorted(cc_list)
    if not verbose:
        cc_iter = tqdm.tqdm(cc_iter)
    for cc in cc_iter:
        try:
            status = download_one(cc,base_url,verbose)
        except httpx.HTTPStatusError as exc:
            error_msg = f'{exc} {type(exc)}'.strip()
        except KeyboardInterrupt:
            break 
        else:
            error_msg = ''

        if error_msg:
            status = DownloadStatus.ERROR
        counter[status] +=1
        if verbose and error_msg:
            print(f'{cc} error: {error_msg}')

    return counter 


from concurrent.futures import as_completed 

DEFAULT_CONCUR_REQ = 30
MAX_CONCUR_REQ = 1000

def download_many(cc_list: list[str],
                  base_url: str,
                  verbose: bool,
                  concur_req:int) -> Counter[DownloadStatus]:
    counter: Counter[DownloadStatus] = Counter()
    with futures.ThreadPoolExecutor(max_workers=concur_req) as executor:
        to_do_map = {}
        for cc in sorted(cc_list):
            future = executor.submit(download_one, cc,
                                     base_url, verbose)
            to_do_map[future] = cc 
        done_iter = as_completed(to_do_map)
        if not verbose:
            done_iter = tqdm.tqdm(done_iter, total=len(cc_list))
        for future in done_iter:
            try:
                status = future.result()
            except httpx.HTTPStatusError as exc:
                error_msg = 'HTTP error {resp.status_code} - {resp.reason_phrase}'
                error_msg = error_msg.format(resp=exc.response)
            except httpx.RequestError as exc:
                error_msg = f'{exc} {type(exc)}'.strip()
            except KeyboardInterrupt:
                break 
            else:
                error_msg = ''
            
            if error_msg:
                status = DownloadStatus.ERROR
            counter[status] += 1
            if verbose and error_msg:
                cc = to_do_map[future]
                print(f'{cc} error: {error_msg}')
    return counter


# if __name__ == '__main__':
#     main(download_many,DEFAULT_CONCUR_REQ,MAX_CONCUR_REQ)