# pylint: disable-all

import time, sys, os
from datetime import date
import pandas as pd
import requests 
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat, chain
from wakeup import WindowsInhibitor
from math import ceil
from copy import deepcopy

# written by Shahzod on 9 Dec 2021
# updated to OOP on 28 Apr 2022
# updated to get through API on 29 Apr 2022

MAX_THREADS = 30
TIMEOUT = 60
USD = float(requests.get('https://cbu.uz/oz/arkhiv-kursov-valyut/json/').json()[0]['Rate'])
NOW = date.today()
FIRST_DAY = NOW.replace(day=1)

PATH = ''
DF_CITIES = pd.read_excel(PATH + 'cities.xlsx')
CATEGORIES = {
        'Вторичный': 13,
        'Новостройки': 13,
        'Аренда': 1147,
        'Земля': 1519
        }
FLAT = ('Общая площадь', 'Этажность дома', 'Этаж', 'Количество комнат', 'Тип строения')
LAND = ('Площадь', 'Тип участка')
OFFERS_URL = 'https://www.olx.uz/api/v1/offers/'
OFFERS_LIMIT = 50


class City:
    __slots__ = ('region', 'name', 'num_pages', 'page', 'payload')

    def __init__(self, args):
        region, region_id, city, city_id = args
        self.region = region
        self.name = city.upper()
        self.num_pages = None
        self.page = None
        self.payload = {'offset': 0,
                    'limit': OFFERS_LIMIT,
                    'currency': 'UZS',
                    'filter_refiners': '',
                    'sl': '17c27f2d5bex6a3f1481'}

        if region == 'Город Ташкент':
            self.payload['region_id'] = 5
            self.payload['city_id'] = 4
            self.payload['district_id'] = city_id
        else:
            self.payload['region_id'] = region_id
            self.payload['city_id'] = city_id


class Market:
    __slots__ = ('name', 'features')
    
    def __init__(self, name):
        self.name = name
        self.features = LAND if name == 'Земля' else FLAT


class Runner:
    def __init__(self, city, market):
        self.city = city  # City object
        self.market = market  # Market object
        self.prices = []
        self.city.payload['category_id'] = CATEGORIES[market.name]
        if market.name == 'Вторичный':
            self.city.payload['filter_enum_type_of_market[0]'] = 'secondary'
        elif market.name == 'Новостройки':
            self.city.payload['filter_enum_type_of_market[0]'] = 'primary'

    def make_chunks(self, page):
        city = deepcopy(self.city)
        city.payload['offset'] = page*OFFERS_LIMIT
        city.page = page
        return city


# def converter(price):
#     # price in 'у.е.'
#     if price[-1] == '.':
#         price = float(price.replace('у.е.','').replace(' ', ''))
#     else:
#         price = float(price.replace('сум','').replace(' ', ''))/USD
#     return price

def converter(price_currency):
    price, currency = price_currency
    if price == 'Обмен':
        return 0

    if currency == "UZS":
        price = price / USD

    return price


def get_city(runner):
    city_name = runner.city.name
    try:
        r = requests.get(OFFERS_URL, runner.city.payload).json()
        num_pages =  ceil(r['metadata']['total_elements'] / OFFERS_LIMIT)
        if num_pages != 0:
            # num_pages = min(2, num_pages)
            runner.city.num_pages = num_pages
            chunks = [runner.make_chunks(page) for page in range(num_pages)]
            threads = min(MAX_THREADS, num_pages)
            with ThreadPoolExecutor(max_workers=threads) as executor:
                results_map = executor.map(get_data, chunks, repeat(runner.market.features))
                runner.prices = list(results_map)
        else:
            print(city_name, 'NO RESULTS')
    except Exception as err:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(city_name, 'ERROR occurred:', err, exc_tb.tb_lineno, flush=True)

    return runner


def get_data(city, features):
    r = requests.get(OFFERS_URL, city.payload).json()
    if r['metadata']['promoted']:
        # Get rid of promoted offers
        organic_offers = list(map(r['data'].__getitem__, r['metadata']['source']['organic']))
    else:
        organic_offers = r['data']

    results = [None] * len(organic_offers)
    for i, offer in enumerate(organic_offers):
        published =  offer['last_refresh_time'][:10]       
        # params = {param['name']: param['value']['label'] for param in offer['params']}
        params = {p['name']: (p['value']['label'] if p['name']!='Цена' 
                    else (p['value']['value'], p['value']['currency'])) 
                    for p in offer['params']}
        got_features = [params.get(feature) for feature in features]
        price = params['Цена']
        results[i] = (price, published, *got_features)

    print(city.name, f'{city.page + 1}/{city.num_pages}' if city.num_pages!=1 else 1)
    return results


if __name__ == '__main__':
    start_time = time.time()
    KVM = 'Квм цена'
    
    workers = max(os.cpu_count()-1, 1)
    print('The number of multiprocessing workers:', workers, '\n')
    workbook = f'House_price_{NOW.strftime("%Y-%m-%d")}.xlsx'

    osSleep = None
    # in Windows, prevent the OS from sleeping while we run
    if os.name == 'nt':
        osSleep = WindowsInhibitor()
        osSleep.inhibit()

    try:
        writer = pd.ExcelWriter(PATH + workbook, engine='xlsxwriter')
        for market_name in CATEGORIES.keys():
            print(market_name.upper(), '\n')
            city_objects = [City(x) for x in DF_CITIES.values]
            market = Market(market_name)
            city_runner = (Runner(city, market) for city in city_objects)

            print('Sending requests...\n')
            processes = min(workers, len(city_objects))
            with ProcessPoolExecutor(max_workers=processes) as executor:
                results = executor.map(get_city, city_runner)

            print('\nCreating a worksheet...')
            df = pd.DataFrame.from_dict({
                    (runner.city.region, runner.city.name, k): price
                    for runner in results
                    for (k, price) in enumerate(chain(*runner.prices))
                }, orient='index', columns=['Цена', 'Дата', *market.features])
            df = df.sort_index()
            df.index = pd.MultiIndex.from_tuples(df.index)
            
            for feature in market.features:
                df[feature] = df[feature].str.replace(f'м²|m²| ', '', regex=True)
                df[feature] = pd.to_numeric(df[feature], errors='ignore')

            df['Цена'] = df['Цена'].apply(converter).round()
            df['Дата'] = pd.to_datetime(df['Дата'], format='%Y-%m-%d').dt.date
            df.drop(df[df['Дата'] < FIRST_DAY].index, inplace=True)

            if market_name != 'Земля':
                df[KVM] = (df['Цена']/df['Общая площадь']).round(2)
                old_len = len(df)
                if market_name != 'Аренда':
                    df = df[(df[KVM]>100) & (df[KVM]<1200)]
                total = []
                cities = sorted(set(df.index.droplevel(2)))  
                for city in cities:
                    tr = df.loc[city].sort_values(KVM)
                    if len(tr) > 3:
                        tr = tr[(tr[KVM] > tr[KVM].quantile(0.05)) & (tr[KVM] < tr[KVM].quantile(0.95))]
                    tr.index = pd.MultiIndex.from_tuples([(*city, i) for i in range(len(tr))])
                    total.append(tr)
                df = pd.concat(total)
                new_len = len(df)
                print(f'\nDROPPED {old_len - new_len} prices out of {old_len}')

            df.index.names = ['Регион', 'Город', '№']
            df.to_excel(writer, sheet_name=market_name)
            print('='*50, '\n')

        print('Scraping is DONE!')
    except PermissionError:
        print(workbook, 'is open. Close it!')
        writer = None
    except Exception as err:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print('ERROR occurred:', err, exc_tb.tb_lineno)
    finally:
        if osSleep:
            osSleep.uninhibit()
        if writer:
            print('\nSaving the workbook...')
            writer.close()

    seconds = time.time() - start_time
    print('Time Taken:', time.strftime("%H:%M:%S", time.gmtime(seconds)), '\n')

