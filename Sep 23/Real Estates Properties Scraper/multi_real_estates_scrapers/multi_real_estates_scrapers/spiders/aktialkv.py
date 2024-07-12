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
            f'output/properties/{name} Properties Scraper.csv': {
                'format': 'csv',
                'fields': ['Address', 'street number', 'Type', 'Rooms', 'Other', 'Size (m2)', 'Agency url',
                           'Agency name'],
                'overwrite': True,
            }
        }
    }

    def start_requests(self):
        total_products = 58
        total_pages = ceil(total_products/24)
        for page in range(1, total_pages + 1):
            url = f'https://www.aktialkv.fi/api/apartments?region[]=5500722&categories[]=5500385&page={page}&forRent=false&offset=0'
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        data = response.json().get('apartments', [])
        for property_json in data:
            item = self.get_item(property_json)
            yield item
            # self.get_item(property_json)

    def get_address(self, response):
        try:
            address_row = json.loads(response.get('address', '')).get('parts', {}).get('address', '')
        except Exception as e:
            print(f"An error occurred: {e}")
            address_row = ''

        address = address_row or response.get('title', '').split()[0]
        return address

    def get_street_number(self, response):
        try:
            street = json.loads(response.get('address', '')).get('parts', {}).get('number', '')
        except Exception as e:
            print(f"An error occurred: {e}")
            street = ''

        street_no = street or response.get('title', '').split()[1]
        return street_no

    def get_price(self, response):
        price = response.get('price', '').replace('€', '').replace(',', '').replace('\xa0', '').strip()

        if price:
            price = '{:,}'.format(int(price))
        else:
            price = ''

        return price + ' €'

    def get_rooms(self, response):
        rooms = response.get('roomcount', '').replace(' huonetta', '').replace('huone', '').strip()
        return rooms

    def get_size(self, response):
        return response.get('propertySize', '').replace(' m²', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.get('url', '')

    def get_static(self, response):
        return 'aktia'
