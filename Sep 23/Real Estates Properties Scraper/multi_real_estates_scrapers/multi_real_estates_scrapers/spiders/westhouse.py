import re
from urllib.parse import urlparse
import json
import scrapy
from .base import BaseSpider


class WesthouseSpider(BaseSpider):
    name = 'westhouse'
    start_urls = ['https://westhouse.fi/asunnot/#asunnot']
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
            data = json.loads(response.css('script:contains("window.VueData"):not([type="text/javascript"])').re_first(
                r'(results.*})').replace('results: ', '') + ']')
        except:
            data = []
            return

        for property_selector in data:
            category = property_selector.get('kategoria', '')
            if 'asunnot' in category:
                item = self.get_item(property_selector)
                yield item
            else:
                continue

    def get_address(self, response):
        address = ''.join(response.get('osoite', '').split()[:1])
        return address

    def get_street_number(self, response):
        address = ''.join(response.get('osoite', '').split()[1:2])
        if '-' in address:
            street_no = re.sub(r'\D-\D', '', address)
        else:
            street_no = ''.join(re.findall(r'\d', address))
        return street_no

    def get_price(self, response):
        price = response.get('price', '').replace('€', '').replace('\xa0', '').replace(',', '').replace(' ', '').strip()
        price = '{:,}'.format(int(price))

        return price + ' €'

    def get_rooms(self, response):
        rooms = response.get('rooms', '')
        return rooms

    def get_size(self, response):
        return response.get('size', '').replace('m²', '').strip()

    def get_agency_url(self, response):
        return response.get('url', '')

    def get_static(self, response):
        return 'westhouse'
