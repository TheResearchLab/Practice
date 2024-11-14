import asyncio
import socket 

from keyword import kwlist 

MAX_KEYWORD_LEN = 4

async def probe(domain: str) -> tuple[str,bool]:
    loop = asyncio.get_running_loop()
    try:
        await loop.getaddrinfo(domain,None)
    except socket.gaierror:
        return (domain,False)
    return (domain,True)

async def main() -> None:
    names = (kw for kw in kwlist if len(kw) <= MAX_KEYWORD_LEN)
    domains = ( f'{name}.dev'.lower() for name in names)
    coros = [probe(domain) for domain in domains]
    for coro in asyncio.as_completed(coros):
        domain, found = await coro 
        mark = '+' if found else ' '
        print(f'{mark} {domain}')

# if __name__ == '__main__':
#     asyncio.run(main())

import asyncio 
from httpx import AsyncClient

from flags import BASE_URL, save_flag, main 

async def download_one(client: AsyncClient, cc: str) -> bytes:
    image = await get_flag(client,cc)
    save_flag(image,f'{cc}.gif')
    print(cc,end=' ',flush=True)
    return cc 

async def get_flag(client: AsyncClient, cc: str) -> bytes:
    url = f'{BASE_URL}/{cc}/{cc}.gif'.lower()
    resp = await client.get(url, timeout=6.1,follow_redirects=True)
    return resp.read()


def download_many(cc_list: list[str]) -> int:
    return asyncio.run(supervisor(cc_list))

async def supervisor(cc_list: list[str]) -> int:
    async with AsyncClient() as client:
        to_do = [download_one(client,cc) for cc in sorted(cc_list)]
        res = await asyncio.gather(*to_do)
    
    return len(res)

if __name__ == '__main__':
    main(download_many)

