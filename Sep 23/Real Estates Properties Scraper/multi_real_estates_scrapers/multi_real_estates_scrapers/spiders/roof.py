import re
from .base import BaseSpider


class RoofSpider(BaseSpider):
    name = 'roof'
    start_urls = ['https://roof.fi/kohteet/?kivi-item-toimeksianto-tyyppi=myyntitoimeksianto&kivi-item-asunto-osoite=Ullanlinna&kivi-item-asunto-pamin=&kivi-item-asunto-pamax=&kivi-item-asunto-type-select=&kivi-item-asunto-hintamin=&kivi-item-asunto-hintamax=&submit=Hae&sort=publish_date--DESC#content']
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

    def get_price(self, response):
        return response.css('.h4::text').get('').replace('€', '').strip().replace(' ', ',') + ' €'

    def get_rooms(self, response):
        rooms = response.css('.details p::text').re_first(r'(\d+)s*h')
        return rooms

    def get_size(self, response):
        return response.css('.bold::text').get('').strip().replace('m²', '').replace('m', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.css('a::attr(href)').get('')

    def get_static(self, response):
        return 'roof'
