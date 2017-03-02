import os
import re
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
        'roleType': 'corporation',
        'nameStartLetter': 'LETTER',
        'page': '1',
        'start': '0',
        'limit': '50000'}
directory = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'Result'))
epoch = datetime.datetime.utcfromtimestamp(0)
total_scraped = 0
details = [('http://www.sfc.hk/publicregWeb/corp/%s/details', 'details'), ('http://www.sfc.hk/publicregWeb/corp/%s/addresses', 'addresses'),
           ('http://www.sfc.hk/publicregWeb/corp/%s/ro', 'ro'), ('http://www.sfc.hk/publicregWeb/corp/%s/rep', 'rep'),
           ('http://www.sfc.hk/publicregWeb/corp/%s/co', 'co')]

def main():
    global data
    def IsInt(s):
        try:
            int(s)
            return True
        except ValueError:
            return False
    print('Enter role type (individual - "i", corporation - "c")')
    role_type = input()
    if role_type != 'i' and role_type != 'c':
        print('Invalid role type')
        main()
    data['roleType'] = 'individual' if role_type == 'i' else 'corporation'
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
    print('Processing, please wait . . .')
    perms = list(itertools.product(types, names_start_letters))
    try:
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(perms))
        responses = loop.run_until_complete(future)._result

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run_details(responses))
        corporation_details = loop.run_until_complete(future)._result
        for items, data in [resp for resp in responses if resp]:
            for corporation in items['items']:
                corporation['details'] = ''
                corporation['addresses'] = ''
                corporation['ro'] = ''
                corporation['rep'] = ''
                corporation['co'] = ''
                for type, j, ceref in corporation_details:
                    if corporation['ceref'] == ceref:
                        corporation[type] = j
            to_csv(items, data)

    except RuntimeError:
        print('Exiting...')
    finally:
        print('Your results in ' + directory)

def to_csv(j, data):
    file_path = 'Type - ' + str(data['ratype']) + ' - Letter - ' + str(data['nameStartLetter'])
    with open(os.path.join(directory, file_path + '.csv'), 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(
            ['CE Reference', 'Name', 'Chinese name', 'Entity type',
             'Is individual', 'is EO', 'is Corporative',
             'is Ri', 'Has active licence', 'Is active eo', 'full address chin',
             'central entity', 'full address', 'details', 'addresses', 'ro',
             'rep', 'co'])
        for item in j['items']:
            address = item['address']
            full_address_chin, central_entity, full_address = None, None, None
            if address:
                full_address_chin = address['fullAddressChin']
                central_entity = address['centralEntity']
                full_address = address['fullAddress']
            writer.writerow([item['ceref'], item['name'],
                             item['nameChi'], item['entityType'],
                             item['isIndi'], item['isEo'],
                             item['isCorp'], item['isRi'],
                             item['hasActiveLicence'], item['isActiveEo'],
                             full_address_chin, central_entity,
                             full_address, item['details'], item['addresses'],
                                              item['ro'], item['rep'],
                                              item['co']])


async def fetch_details(url, type, ceref, det_session):
    async with det_session.get(url) as response:
        content = await response.text()
        def get_json(regexp):
            res = re.findall(regexp, content)
            if res:
                return re.findall(regexp, content)[0]
            else:
                return ''
        matches = ''
        if type == 'details':
            matches = get_json('(?<=var raDetailData = )(.*)(?=;)')
        if type == 'addresses':
            address_data = get_json('(?<=var addressData = \[)(.*)(?=];)')
            email_data = get_json('(?<=var emailData = \[)(.*)(?=];)')
            website_data = get_json('(?<=var websiteData = \[)(.*)(?=];)')
            matches = ','.join(item for item in [address_data, email_data, website_data] if item)
        if type == 'ro':
            matches = get_json('(?<=var roData = )(.*)(?=;)')
        if type == 'rep':
            matches = get_json('(?<=var repData = )(.*)(?=;)')
        if type == 'co':
            matches = get_json('(?<=var cofficerData = )(.*)(?=;)')
        print('Scraped corporation ' + type + '  with ceref ' + ceref)
        return type, matches, ceref


async def bound_details(sem, url, type, ceref, det_session):
    try:
        async with sem:
            return await fetch_details(url, type, ceref, det_session)
    except aiohttp.errors.ClientOSError:
        print(str(data) + ' - ERROR')
        async with sem:
            return await fetch_details(url, type, ceref, det_session)

async def run_details(corp_chunks):
    tasks = []
    sem = asyncio.Semaphore(50)

    async with ClientSession() as det_session:
        for items, data in [chunk for chunk in corp_chunks if chunk]:
            for corporation in items['items']:
                for url, type in details:
                    task = asyncio.ensure_future(bound_details(sem, url % corporation['ceref'], type, corporation['ceref'], det_session))
                    tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        return responses

async def fetch(data, session):
    try:
        param = int((datetime.datetime.now() - epoch).total_seconds() * 1000.0)
        url = main_url % param
        async with session.post(url, headers=headers, data=data, timeout=500) as response:
            global total_scraped
            total_scraped += 1

            j = await response.text()
            try:
                j = json.loads(j)
                print('Total scraped ' + str(total_scraped) + ' . Items count : ' + str(len(j['items'])))
                return j, data
            except json.JSONDecodeError:
                return None
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
    sem = asyncio.Semaphore(50)

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
        return responses



if __name__ == '__main__':
    main()