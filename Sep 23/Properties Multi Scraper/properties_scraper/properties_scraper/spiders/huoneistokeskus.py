import re
from .base import BaseSpider
from scrapy import Request


class HuoneistokeskusSpider(BaseSpider):
    name = 'huoneistokeskus'
    # start_urls = ['https://www.huoneistokeskus.fi/myytavat-asunnot?Location=Ullanlinna&&&NewProperty=include&service=Residences&top=12']

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
        filename = 'huoneistokeskus.txt'
        self.urls_from_input_file = self.read_input_urls(filename=filename)

    def start_requests(self):
        for url in self.urls_from_input_file:
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        properties_selectors = response.css('.search-result-list  .list-link')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        address = re.sub(r'\d', '', response.css('.title::text').get('').split(',')[0])
        return address.replace('-', '')

    def get_street_number(self, response):
        address = response.css('.title::text').get('').split(',')[0]
        if '-' in address:
            street_number = ''.join(re.findall(r'\d+-\d+', address))
        else:
            street_number = ''.join(re.findall(r'\d', address))

        # street_no = ''.join(re.findall(r'\d', address_row))
        return street_number

    def get_type(self, response):
        return response.css('.right.price::text').get('')

    def get_rooms(self, response):
        rooms = response.css('.room-specs::text').re_first(r'(\d+)s*h') or response.css('.room-specs::text').re_first(r'(\d+)s*')
        return rooms

    def get_size(self, response):
        return response.css('.left::text ').get('').strip().replace('m²', '').replace('m', '')

    def get_agency_url(self, response):
        return response.css('.list-link::attr(href)').get('')

    def get_static(self, response):
        return 'huoneistokeskus'
