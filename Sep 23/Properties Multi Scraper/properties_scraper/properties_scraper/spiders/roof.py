import re
from .base import BaseSpider
from scrapy import Request


class RoofSpider(BaseSpider):
    name = 'roof'
    # start_urls = [
    #     'https://roof.fi/kohteet/?kivi-item-toimeksianto-tyyppi=myyntitoimeksianto&kivi-item-asunto-osoite=Ullanlinna&kivi-item-asunto-pamin=&kivi-item-asunto-pamax=&kivi-item-asunto-type-select=&kivi-item-asunto-hintamin=&kivi-item-asunto-hintamax=&submit=Hae&sort=publish_date--DESC#content']

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
        filename = 'roof.txt'
        self.urls_from_input_file = self.read_input_urls(filename=filename)

    def start_requests(self):
        for url in self.urls_from_input_file:
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        properties_selectors = response.css('.card-kivi_item')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        return response.css('.default strong::text').get('').split()[0]

    def get_street_number(self, response):
        address_row = response.css('.default strong::text').get('')
        street_no = ''.join(re.findall(r'\d', address_row))
        return street_no

    def get_type(self, response):
        return response.css('.h4::text').get('').strip()

    def get_rooms(self, response):
        rooms = response.css('.details p::text').re_first(r'(\d+)s*h')
        return rooms

    def get_size(self, response):
        return response.css('.bold::text').get('').strip().replace('m²', '').replace('m', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.css('a::attr(href)').get('')

    def get_static(self, response):
        return 'roof'
