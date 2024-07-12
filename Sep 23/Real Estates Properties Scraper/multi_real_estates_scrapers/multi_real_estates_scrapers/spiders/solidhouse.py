import re
from urllib.parse import urlparse, urljoin

import scrapy
from .base import BaseSpider


class SolidhouseSpider(BaseSpider):
    name = 'solidhouse'
    start_urls = ['https://www.solidhouse.fi/myytavat-asunnot']
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
        properties_selectors = response.css('#property-list div.property')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            if 'k' not in item.get('Type', ''):
                yield item

    def get_address(self, response):
        address_row = response.css('p::text').getall()[1]
        # address_row = response.xpath('//p/br[1]/following-sibling::text()').get('')
        address = re.sub(r'\d.*', '', address_row).strip().replace('-', '')
        return address

    def get_street_number(self, response):
        address_row = response.css('p::text').getall()[1]
        street_no = ''.join(re.findall(r'\d-\d', address_row)) or ''.join(re.findall(r'\d', address_row))
        return street_no

    def get_price(self, response):
        return response.css('p + p::text').get('').replace('€', '').replace('Vh.', '').strip().replace(' ', ',') + ' €'

    def get_size(self, response):
        return response.css('h3 + h3::text').get('').strip().replace('m²', '').replace('m', '')

    def get_agency_url(self, response):
        url = 'https://www.solidhouse.fi/'
        return urljoin(url, response.css('a::attr(href)').get(''))

    def get_static(self, response):
        url = 'https://www.solidhouse.fi/'
        return urlparse(url).netloc.split('.')[1]
