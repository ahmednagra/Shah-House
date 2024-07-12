import re

from .base import BaseSpider


class QvadratSpider(BaseSpider):
    name = 'qvadrat'

    start_urls = ['https://www.qvadrat.fi/wp-json/linear/v1_1/listings/all?lang=fi']

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'FEEDS': {
            f'output/properties/{name} Properties.csv': {
                'format': 'csv',
                'overwrite': True,
            }
        }
    }

    def parse(self, response, **kwargs):
        try:
            data = response.json()
        except:
            data = []
            return

        for property_selector in data:
            permalink = property_selector.get('permalink', '')
            if 'myynti' in permalink and property_selector.get('debtFreePrice', ''):
                item = self.get_item(property_selector)
                yield item
            else:
                continue

    def get_address(self, response):
        return response.get('address', '').split()[0]

    def get_street_number(self, response):
        address = response.get('address', '')

        if '-' in address:
            street_number = ''.join(re.findall(r'\d+-\d+', address))
        else:
            street_number = ''.join(re.findall(r'\d', address))

        return street_number

    def get_type(self, response):
        return response.get('debtFreePrice', '').strip()

    def get_rooms(self, response):
        rooms = response.get('roomCount', 0) or ''.join(re.findall(r'\d', response.get('roomCount', '')))
        return rooms

    def get_size(self, response):
        size = response.get('area', '')

        if size is not None:
            size = size.replace('m²', '').replace(',', '.').strip()
        else:
            size = ''

        return size

    def get_agency_url(self, response):
        return response.get('permalink', '')

    def get_static(self, response):
        return 'qvadrat'
