from unicodecsv import csv
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


def main():
    perms = list(itertools.product(types, names_start_letters))
    print(len(perms))
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(perms))
    loop.run_until_complete(future)


async def fetch(url, data, session):
    async with session.post(url, headers=headers, data=data, timeout=500) as response:
        print('REQUEST: ' + url + ' . ' + str(data['ratype']) + ' - ' + str(data['nameStartLetter']))
        j = await response.text()
        j = json.loads(j)
        return j


async def bound_fetch(sem, url, data, session):
    async with sem:
        return await fetch(url, data, session)


async def run(perms):
    tasks = []
    sem = asyncio.Semaphore(30)

    async with ClientSession() as session:
        for pair in perms[1:10]:
            epoch = datetime.datetime.utcfromtimestamp(0)
            param = int((datetime.datetime.now() - epoch).total_seconds() * 1000.0)
            p_data = data.copy()
            p_data['ratype'] = pair[0]
            p_data['nameStartLetter'] = pair[1]
            task = asyncio.ensure_future(bound_fetch(sem, main_url % param, p_data, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        result = await responses
        with open('result.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(
                ['CE Reference', 'Name', 'Chinese name', 'Entity type', 'Is individual', 'is EO', 'is Corporative',
                 'is Ri',
                 'Has active licence', 'Is active eo', 'Address'])
            for d in result:
                for item in d['items']:
                    writer.writerow([item['ceref'], item['name'],
                                     item['nameChi'], item['entityType'],
                                     item['isIndi'], item['isEo'],
                                     item['isCorp'], item['isRi'],
                                     item['hasActiveLicence'], item['isActiveEo'],
                                     item['address']])


if __name__ == '__main__':
    main()
