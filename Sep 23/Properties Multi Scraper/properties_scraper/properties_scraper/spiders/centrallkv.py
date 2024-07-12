import re
from urllib.parse import urlparse
from .base import BaseSpider


class centrallkvSpider(BaseSpider):
    name = 'centrallkv'
    start_urls = ['https://centrallkv.fi/fi/myyntikohteet.html']

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
        properties_selectors = response.css('.fm2group .item')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        address_row = response.css('.header h3::text').get('')
        address = re.sub(r'\d', '', address_row).strip()
        return address.replace('-', '')

    def get_street_number(self, response):
        address = ''.join(response.css('.header h3::text').get('').split()[1:2])
        if '-' in address:
            street_no = re.sub(r'\D-\D', '', address)
        else:
            street_no = ''.join(re.findall(r'\d', address))
        return street_no

    def get_type(self, response):
        # price = response.css('.price::text').get('').replace('\xa0', '').replace('€', ' ').replace(' ', '').strip()
        # price = float(price)
        # formatted_price = "{:,.2f}".format(price).replace(',', ' ').replace('.', ',')

        return ' '.join([x.strip() for x in response.css('.price::text').get('').split(' ')])


    def get_rooms(self, response):
        return response.css('.room-types::text').re_first(r'(\d+)[sh]*')

    def get_size(self, response):
        return response.css('.area::text').get('').strip().replace('m²', '').replace('m', '')

    def get_agency_url(self, response):
        return response.css('.apartment-link::attr(href)').get('')

    def get_static(self, response):
        url = response.css('.apartment-link::attr(href)').get('')
        return urlparse(url).netloc.split('.')[0]
