import os
from unicodecsv import csv
import json
import itertools
import datetime
import asyncio
import aiohttp
from aiohttp import ClientSession

main_url = 'http://www.sfc.hk/publicregWeb/searchByRaJson?_dc=%s'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
names_start_letters = [letter.upper() for letter in list(map(chr, range(97, 123)))] + list(
    [str(i) for i in range(0, 11)])
data = {'licstatus': 'all',
        'ratype': 'RATYPE',
        'roleType': 'individual',
        'nameStartLetter': 'LETTER',
        'page': '1',
        'start': '0',
        'limit': '50000'}
directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Result')
epoch = datetime.datetime.utcfromtimestamp(0)
total_scraped = 0


def main():
    def IsInt(s):
        try:
            int(s)
            return True
        except ValueError:
            return False
    print('Enter from type ( 1 - 10 )')
    type_from = input()
    if not IsInt(type_from):
        print('Type should be number')
        return
    type_from = int(type_from)
    print('Enter to type ( 1 - 10 )')
    type_to = input()
    if not IsInt(type_to):
        print('Type should be number')
        return
    type_to = int(type_to)
    if type_from > 10 or type_from < 1:
        print('Enter valid type ( 1 - 10 )')
        return
    if type_to > 10 or type_to < 1:
        print('Enter valid type ( 1 - 10 )')
        return
    if type_to < type_from:
        type_from, type_to = type_to, type_from
    types = list(range(type_from, type_to + 1))
    print('Script will scrape:')
    print('Types: ' + str(types))
    print('Name starts with: A-Z and 1-9')
    perms = list(itertools.product(types, names_start_letters))
    try:
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(perms))
        loop.run_until_complete(future)
    except RuntimeError:
        print('Exiting...')
    finally:
        print('Your results in ' + directory)


async def fetch(data, session):
    try:
        param = int((datetime.datetime.now() - epoch).total_seconds() * 1000.0)
        url = main_url % param
        async with session.post(url, headers=headers, data=data, timeout=500) as response:
            global total_scraped
            total_scraped += 1
            file_path = 'Type -' + str(data['ratype']) + ' - Letter - ' + str(data['nameStartLetter'])
            print(file_path + ' . Total scraped ' + str(total_scraped))
            j = await response.text()
            try:
                j = json.loads(j)
            except json.JSONDecodeError:
                return
            with open(os.path.join(directory, file_path + '.csv'),
                      'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(
                    ['CE Reference', 'Name', 'Chinese name', 'Entity type',
                     'Is individual', 'is EO', 'is Corporative',
                     'is Ri', 'Has active licence', 'Is active eo', 'Address'])
                for item in j['items']:
                    writer.writerow([item['ceref'], item['name'],
                                     item['nameChi'], item['entityType'],
                                     item['isIndi'], item['isEo'],
                                     item['isCorp'], item['isRi'],
                                     item['hasActiveLicence'], item['isActiveEo'],
                                     item['address']])
    except:
        pass


async def bound_fetch(sem, data, session):
    try:
        async with sem:
            return await fetch(data, session)
    except aiohttp.errors.ClientOSError:
        print(str(data) + ' - ERROR')
        async with sem:
            return await fetch(data, session)


async def run(perms):
    tasks = []
    sem = asyncio.Semaphore(30)

    if not os.path.exists(directory):
        os.makedirs(directory)

    async with ClientSession() as session:
        for pair in perms:
            p_data = data.copy()
            p_data['ratype'] = pair[0]
            p_data['nameStartLetter'] = pair[1]
            task = asyncio.ensure_future(bound_fetch(sem, p_data, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses



if __name__ == '__main__':
    main()
