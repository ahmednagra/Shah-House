import re
from math import ceil

from scrapy import Request

from .base import BaseSpider


class BlokSpider(BaseSpider):
    name = 'blok'

    start_urls = [
        'https://api.blok.ai/listings/?&neighborhood=Ullanlinna&country=FI&type=&lang=en&sold=false&new=false&page=1&size=29&order_by=-published_at&area__lte=&area__gte=&price__gte=&price__lte=']

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

    def parse(self, response, **kwargs):
        try:
            data = [x.get('data', {}) for x in response.json().get('results', {}).get('items', [])]
        except:
            data = []
            return

        for property_selector in data:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        return response.get('location_street_address', '').split()[0]

    def get_street_number(self, response):
        return ''.join(re.findall(r'\d', response.get('location_street_address', '')))

    def get_price(self, response):
        # price = '{:,}'.format(int(response.get('price', '')))
        price = response.get('price_display', '').replace('€', '').replace('\xa0', '').replace(' ', '').strip()
        price = '{:,}'.format(int(price))

        return price + ' €'

    def get_rooms(self, response):
        rooms = response.get('room_count', '').split('.')[0]
        return rooms

    def get_size(self, response):
        return response.get('area_living', '').split('.')[0]

    def get_agency_url(self, response):
        return f'{"https://blok.ai/en/property/"}{response.get("slug")}'

    def get_static(self, response):
        return 'blok'
