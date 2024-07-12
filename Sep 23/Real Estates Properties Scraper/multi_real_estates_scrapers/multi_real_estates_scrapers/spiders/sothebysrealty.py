import re
from .base import BaseSpider


class SothebysrealtySpider(BaseSpider):
    name = 'sothebysrealty'
    start_urls = ['https://sothebysrealty.fi/kohteet/']
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
        properties_selectors = response.css('.status-publish')
        for property_selector in properties_selectors:
            item =self.get_item(property_selector)
            yield item

    def get_address(self, response):
        address = response.css('.apartment-address::text').re_first(r'(.*?)\d').strip()
        if not address:
            address = response.css('.apartment-address::text').get('').strip()

        return address

    def get_street_number(self, response):
        address = response.css('.apartment-address::text').get('')
        if '-' in address:
            street_no = re.sub(r'[^\d-]', '', address)
        else:
            street_no = ''.join(re.findall(r'\d', address))
        return street_no

    def get_price(self, response):
        return response.css('.apartment-sales_price::text').get('').replace('€', '').strip().replace(' ', ',') + ' €'

    def get_size(self, response):
        return response.css('.apartment-total_area::text').get('').strip().replace('m²', '').replace('m', '')

    def get_agency_url(self, response):
        return response.css('.apartment-info a::attr(href)').get('')

    def get_static(self, response):
        return 'sothebys'
