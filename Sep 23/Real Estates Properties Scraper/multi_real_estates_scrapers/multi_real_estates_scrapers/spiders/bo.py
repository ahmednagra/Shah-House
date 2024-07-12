import re
from urllib.parse import urlparse
from scrapy import Spider, Request
from .base import BaseSpider


class BospiderSpider(BaseSpider):
    name = 'bospider'
    start_urls = [
        'https://bo.fi/asunnot/?uusi-alue%5B%5D=Ullanlinna&uusi-alue%5B%5D=Eira&uusi-alue%5B%5D=Kaivopuisto&uusi-alue%5B%5D=Kaartinkaupunki&hinta-min=&hinta-max=&pinta-ala-min=&pinta-ala-max=&ala=asuin&rakennusvuosi-min=&rakennusvuosi-max=&uudis=kaikki&jarjesta=id']

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
        properties_selectors = response.css('.realty-grid__content .realty-grid__single')
        for property_selector in properties_selectors:
            item = self.get_item(property_selector)
            yield item

        next_page = response.css('.right::attr(href)').get('')
        if next_page:
            yield Request(url=response.urljoin(next_page), callback=self.parse)

    def get_address(self, response):
        address_row = response.css('.realty-grid__single__content__address h3::text').get('')
        address = re.sub(r'\d', '', address_row).strip()
        return address

    def get_street_number(self, response):
        address = ''.join(response.css('.realty-grid__single__content__address h3::text').get('').split()[1:2])

        if '-' in address:
            street_no = re.sub(r'\D-\D', '', address)
        else:
            street_no = ''.join(re.findall(r'\d', address))
        return street_no

    def get_price(self, response):
        return response.css('.realty-grid__single__content__price span::text').get('')\
            .replace('€', '').strip().replace(' ', ',') + ' €'

    def get_rooms(self, response):
        rooms = response.css('.realty-grid-oneliner::text').re_first(r'(\d+)s*h')
        rooms = rooms or response.css('.realty-grid-oneliner::text').re_first(r'(\d+)s* h')

        return rooms

    def get_size(self, response):
        return response.css('.realty-grid__single__content__price span + span::text ').get('').strip()\
            .replace('m²', '').replace('m', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.css('a::attr(href)').get('')

    def get_static(self, response):
        url = response.css('a::attr(href)').get('')
        return urlparse(url).netloc.split('.')[0]
