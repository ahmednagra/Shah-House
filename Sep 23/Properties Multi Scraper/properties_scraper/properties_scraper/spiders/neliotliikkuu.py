import re

from scrapy import Request

from .base import BaseSpider


class NeliotliikkuuSpider(BaseSpider):
    name = 'neliotliikkuu'
    # start_urls = [
    #     'https://neliotliikkuu.fi/myytavat-asunnot/?_realtytype=&kivi-item-asunto-osoite=Helsinki&kivi-item-asunto-hintamin=&kivi-item-asunto-hintamax=&kivi-item-asunto-pamin=&submit=Hae']

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
        filename = 'neliotliikkuu.txt'
        self.urls_from_input_file = self.read_input_urls(filename=filename)

    def start_requests(self):
        for url in self.urls_from_input_file:
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        properties_selectors = response.css('.kivi-index-item.itemgroup-asunnot')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

        next_page = response.css('.next.page-numbers::attr(href)').get('')
        if next_page:
            yield Request(url=next_page, callback=self.parse)

    def get_address(self, response):
        return response.css('h2.limit-2::text').re_first(r'(.*?)\d').replace('-', '').strip()

    def get_street_number(self, response):
        street = response.css('h2.limit-2::text').re_first(r'(\d{1,9}-\d{1,9})')
        street = street or response.css('h2.limit-2::text').re_first(r'(\d{1,2})')

        return street

    def get_type(self, response):
        return response.css('[title="Hinta"]::text').get('').strip().split('\n')[1].strip()

    def get_rooms(self, response):
        rooms = response.css('.kivi-item-body__structure::text').re_first(r'(\d+)s*h')
        rooms = rooms or response.css('.kivi-item-body__structure::text').re_first(r'(\d+)')

        return rooms

    def get_size(self, response):
        return response.css('[title="Koko"]::text').re_first(r'(\d.*)').replace('m²', '').replace(',', '.').strip()

    def get_agency_url(self, response):
        return response.css('a::attr(href)').get('')

    def get_static(self, response):
        return 'neliotliikkuu'
