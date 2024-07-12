import re
from math import ceil

from scrapy import Request

from .base import BaseSpider


class RemaxSpider(BaseSpider):
    name = 'remax'
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

    def start_requests(self):
        cities_list = ['Ullanlinna', 'Eira', 'Kaartinkaupunki', 'Kaivopuisto']
        for city in cities_list:
            url = f'https://remax.fi/wp-content/themes/blocksy-child/property_search_LINEAR.php?property-type=asunnot&realty-type=&bedrooms=&location={city}&price_min=&price_max=&living_area_m2_min=&living_area_m2_max=&lot_area_min=&lot_area_max=&buildyear_min=&buildyear_max='
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            data = response.json()
        except Exception as e:
            print(f"An error occurred: {e}")
            data = []
            return

        for property_selector in data:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        return response.get('street_address', '').split()[0]

    def get_street_number(self, response):
        address = response.get('street_address', '').split()[1]
        street_no = re.sub(r'\D-\D', '', address) or re.sub(r'\d', '', address)
        return street_no

    def get_price(self, response):
        price = response.get('price', '').replace('€', '').replace('\xa0', '').replace(',', '').replace(' ', '').strip()
        price = '{:,}'.format(int(price))

        return price + ' €'

    def get_rooms(self, response):
        rooms = response.get('roomAmount', 0)
        rooms = rooms or re.search(r'(\d+)[sh]*', response.get('rooms', '')) and re.search(r'(\d+)[sh]*', response.get('rooms', '')).group(1) or ''

        return rooms

    def get_size(self, response):
        return response.get('totalArea', None)

    def get_agency_url(self, response):
        town = response.get("town",'')
        district = response.get("district", '')
        listingType = response.get("listingType", '')
        identifier = response.get("identifier", '')
        url = f'{"https://remax.fi/kohde/"}{town}/{district}/{listingType}/{identifier}'

        return response.get('canonicalUrl', '') or url

    def get_static(self, response):
        return 'kiinteistomaailma'
