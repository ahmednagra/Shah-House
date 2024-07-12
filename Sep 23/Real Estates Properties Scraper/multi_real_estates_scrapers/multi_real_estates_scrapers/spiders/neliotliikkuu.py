import re

from scrapy import Request

from .base import BaseSpider


class neliotliikkuuSpider(BaseSpider):
    name = 'neliotliikkuu'
    start_urls = ['https://neliotliikkuu.fi/myytavat-asunnot/?_realtytype=&kivi-item-asunto-osoite=Helsinki&kivi-item-asunto-hintamin=&kivi-item-asunto-hintamax=&kivi-item-asunto-pamin=&submit=Hae']
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
        properties_selectors = response.css('.kivi-index-item.itemgroup-asunnot')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item
        next_page = response.css('.next.page-numbers::attr(href)').get('')
        if next_page:
            yield Request(url=next_page, callback=self.parse)

    def get_address(self, response):
        return response.css('h2.limit-2::text').re_first(r'(.*?)\d').strip()

    def get_street_number(self, response):
        street = response.css('h2.limit-2::text').re_first(r'(\d-\d)')
        street = street or response.css('h2.limit-2::text').re_first(r'(\d)')

        return street

    def get_price(self, response):
        price = response.css('[title="Hinta"]::text').re_first(r'(\d.*)')
        price = re.sub(r'[\s€]*', '', price)
        price = '{:,}'.format(int(price))

        return price + ' €'

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
