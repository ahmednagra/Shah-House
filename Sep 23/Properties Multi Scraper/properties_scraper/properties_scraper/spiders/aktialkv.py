import json
import re
from math import ceil

from scrapy import Request

from .base import BaseSpider


class AktialkvSpider(BaseSpider):
    name = 'aktialkv'
    start_urls = ['https://www.aktialkv.fi/myytavat-asunnot/helsinki/page-3?categories[]=5500385']

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'FEEDS': {
            f'output/properties/{name} Properties.csv': {
                'format': 'csv',
                'overwrite': True,
            }
        }
    }

    def start_requests(self):
        # There is no defined number of products or pages on the website
        total_products = 100
        total_pages = ceil(total_products/24)

        for page in range(1, total_pages + 1):
            url = f'https://www.aktialkv.fi/api/apartments?region[]=5500722&categories[]=5500385&page={page}&forRent=false&offset=0'
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            data = response.json().get('apartments', [])
            for property_json in data:
                item = self.get_item(property_json)
                yield item

        except Exception as e:
            print("Caught a general exception:", e)

    def get_address(self, response):
        try:
            address_row = json.loads(response.get('address', '')).get('parts', {}).get('address', '')
        except Exception as e:
            address_row = ''

        address = address_row or response.get('title', '').split()[0]
        return address

    def get_street_number(self, response):
        try:
            street = json.loads(response.get('address', '')).get('parts', {}).get('number', '')
            street = re.sub(r'[^0-9]', '', street)

            if not street:
                street = ''
        except Exception as e:
            street = ''

        return street

    def get_type(self, response):
        price = response.get('price', '').replace('\xa0', '').replace('€', '').replace('.', ',')

        if price:
            price = price.split(',')[0]
            formatted_price = '{:,}'.format(int(price)).replace(',', ' ')
            return formatted_price + ' €'
        else:
            return ''

    def get_rooms(self, response):
        rooms = response.get('roomcount', '').replace('huonetta', '').replace('huone', '').strip()
        return rooms

    def get_size(self, response):
        return response.get('propertySize', '').replace(' m²', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.get('url', '')

    def get_static(self, response):
        return 'aktia'
