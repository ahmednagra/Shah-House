import re
from urllib.parse import urlparse

from .base import BaseSpider


class KotimeklaritSpider(BaseSpider):
    name = "kotimeklarit"
    start_urls = ["https://kotimeklarit.com/myynnissa/"]

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
        properties_selectors = response.css('.view-apartments .appartment-list')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        return response.css('.info h2::text').get('').split()[0]

    def get_street_number(self, response):
        address = response.css('.info h2::text').get('').split()[1]
        if '-' in address:
            street_no = re.sub(r'\D-\D', '', address)
        else:
            street_no = ''.join(re.findall(r'\d', address))
        return street_no

    def get_type(self, response):
        price = ''.join(response.css('.price::text').get('').split(',')[0]) + ' €'
        return price

    def get_rooms(self, response):
        rooms = response.css('.room-types::text').re_first(r'(\d+)[sh]*')
        return rooms

    def get_size(self, response):
        return response.css('.area::text').get('').strip().replace('m²', '').replace('m', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.css('a::attr(href)').get('')

    def get_static(self, response):
        url = response.css('a::attr(href)').get('')
        return urlparse(url).netloc.split('.')[0]
