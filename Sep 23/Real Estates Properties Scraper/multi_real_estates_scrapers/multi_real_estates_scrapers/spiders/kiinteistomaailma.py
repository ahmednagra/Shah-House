import re

from scrapy import Request

from .base import BaseSpider


class KiinteistomaailmaSpider(BaseSpider):
    name = 'kiinteistomaailma'
    start_urls = ['https://www.kiinteistomaailma.fi/api/km/KM/?areaType=living&limit=30&maxArea&maxYearBuilt&minArea&minYearBuilt&rental=false&sort=latestPublishTimestamp&sortOrder=desc&type=property&query[]=%7B%22district%22%3A%22Ullanlinna%22%2C%22city%22%3A%22Helsinki%22%7D&query[]=%7B%22district%22%3A%22Eira%22%2C%22city%22%3A%22Helsinki%22%7D&query[]=%7B%22district%22%3A%22Kaartinkaupunki%22%2C%22city%22%3A%22Helsinki%22%7D']
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
        data = response.json().get('data', {}).get('results', [])

        for property_selector in data:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        return response.get('address', '').split()[0]

    def get_street_number(self, response):
        address = response.get('address', '').split()[1]
        street_no = re.sub(r'\D-\D', '', address) or re.sub(r'\d', '', address)
        return street_no

    def get_price(self, response):
        price = '{:,}'.format(int(response.get('salesPriceUnencumbered', 0)))

        return price + ' €'

    def get_rooms(self, response):
        rooms = response.get('roomAmount', 0)
        return rooms

    def get_size(self, response):
        return response.get('totalArea', None)

    def get_agency_url(self, response):
        return response.get('canonicalUrl', '')

    def get_static(self, response):
        return 'kiinteistomaailma'
