import requests
import csv
import json
import itertools
import datetime
import asyncio
from aiohttp import ClientSession

main_url = 'http://www.sfc.hk/publicregWeb/searchByRaJson?_dc=%s'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
names_start_letters = [letter.upper() for letter in list(map(chr, range(97, 123)))] + list(
    [str(i) for i in range(0, 11)])
types = list(range(1, 11))
data = {'licstatus': 'all',
        'ratype': 'RATYPE',
        'roleType': 'individual',
        'nameStartLetter': 'LETTER',
        'page': '1',
        'start': '0',
        'limit': '50000'}
csvfile = open('result.csv', 'w', newline='')
writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)


def main():
    perms = list(itertools.product(types, names_start_letters))
    print(len(perms))
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(perms))
    loop.run_until_complete(future)


async def fetch(pair, session):
    epoch = datetime.datetime.utcfromtimestamp(0)
    param = int((datetime.datetime.now() - epoch).total_seconds() * 1000.0)
    p_data = data.copy()
    p_data['ratype'] = pair[0]
    p_data['nameStartLetter'] = pair[1]
    async with session.post(main_url % param, headers=headers, data=p_data, timeout=500) as response:
        print('REQUEST: ' + main_url % param + ' . PAIR: ' + str(pair))
        j = await response.text()
        j = json.loads(j)
        for item in j['items']:
            writer.writerow([item['ceref'], item['name'],
                              item['nameChi'], item['entityType'],
                              item['isIndi'], item['isEo'],
                              item['isCorp'], item['isRi'],
                              item['hasActiveLicence'], item['isActiveEo'],
                              item['address']])
            writer.flush()


async def bound_fetch(sem, pair, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(pair, session)


async def run(perms):
    writer.writerow(
        ['CE Reference', 'Name', 'Chinese name', 'Entity type', 'Is individual', 'is EO', 'is Corporative', 'is Ri',
         'Has active licence', 'Is active eo', 'Address'])

    tasks = []
    sem = asyncio.Semaphore(30)

    async with ClientSession() as session:
        for pair in perms:
            task = asyncio.ensure_future(bound_fetch(sem, pair, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses


if __name__ == '__main__':
    main()
