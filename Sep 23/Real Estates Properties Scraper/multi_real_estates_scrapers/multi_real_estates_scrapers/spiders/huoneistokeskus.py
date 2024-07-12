import re
from .base import BaseSpider


class HuoneistokeskusSpider(BaseSpider):
    name = 'huoneistokeskus'
    start_urls = ['https://www.huoneistokeskus.fi/myytavat-asunnot?Location=Ullanlinna&&&NewProperty=include&service=Residences&top=12']

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
        properties_selectors = response.css('.search-result-list  .list-link')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        return re.sub(r'\d', '', response.css('.title::text').get('').split(',')[0])

    def get_street_number(self, response):
        address_row = response.css('.title::text').get('').split(',')[0]
        street_no = ''.join(re.findall(r'\d', address_row))
        return street_no

    def get_price(self, response):
        return response.css('.right.price::text').get('').replace('€', '').strip().replace(' ', ',') + ' €'

    def get_rooms(self, response):
        rooms = response.css('.room-specs::text').re_first(r'(\d+)s*h') or response.css('.room-specs::text').re_first(r'(\d+)s*')
        return rooms

    def get_size(self, response):
        return response.css('.left::text ').get('').strip().replace('m²', '').replace('m', '')

    def get_agency_url(self, response):
        return response.css('.list-link::attr(href)').get('')

    def get_static(self, response):
        return 'huoneistokeskus'
