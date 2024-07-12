import re

from .base import BaseSpider
from scrapy import Request


class BlokSpider(BaseSpider):
    name = 'blok'

    # start_urls = [
    #     'https://api.blok.ai/listings/?&neighborhood=Ullanlinna&country=FI&type=&lang=en&sold=false&new=false&page=1&size=29&order_by=-published_at&area__lte=&area__gte=&price__gte=&price__lte=']

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'FEEDS': {
            f'output/properties/{name} Properties.csv': {
                'format': 'csv',
                'overwrite': True,
            }
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        filename = 'blok.txt'
        self.urls_from_input_file = self.read_input_urls(filename=filename)

    def start_requests(self):
        for url in self.urls_from_input_file:
            url_split = url.split('?neighborhood=')[1]
            url = f'https://api.blok.ai/listings/?&neighborhood={url_split} + &country=FI&type=&lang=en&sold=false&new=false&size=29&order_by=-published_at&area__lte=&area__gte=&price__gte=&price__lte='
            yield Request(url=url, callback=self.parse)

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
        address_row = response.get('location_street_address', '')
        address_r = re.sub(r'\d', '', address_row)
        address = address_r.replace('-', '').strip()

        if not address:
            address = response.get('location_street_address', '').split()[0]

        return address

    def get_street_number(self, response):
        address = response.get('location_street_address', '')

        if '-' in address:
            street_no = re.sub(r'^.*?(\d+-\d+).*?$', r'\1', address)
        else:
            street_no = ''.join(re.findall(r'\d', address))

        return street_no

    def get_type(self, response):
        return response.get('price_display', '')

    def get_rooms(self, response):
        rooms = response.get('room_count', '').split('.')[0]
        return rooms

    def get_size(self, response):
        return response.get('area_living', '').split('.')[0]

    def get_agency_url(self, response):
        return f'{"https://blok.ai/en/property/"}{response.get("slug")}'

    def get_static(self, response):
        return 'blok'
