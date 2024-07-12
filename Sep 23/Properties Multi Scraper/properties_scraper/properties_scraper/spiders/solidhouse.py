import re
from urllib.parse import urlparse, urljoin

from .base import BaseSpider


class SolidhouseSpider(BaseSpider):
    name = 'solidhouse'
    start_urls = ['https://www.solidhouse.fi/myytavat-asunnot']

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
        properties_selectors = response.css('#property-list div.property')

        for property_selector in properties_selectors:
            item = self.get_item(property_selector)

            if 'k' not in item.get('Type', ''):
                yield item

    def get_address(self, response):
        address_row = response.css('p::text').getall()[1]
        address = re.sub(r'\d.*', '', address_row).strip().replace('-', '')

        return address

    def get_street_number(self, response):
        address_row = response.css('p::text').getall()[1]
        street_no = ''.join(re.findall(r'\d{1,9}-\d{1,9}', address_row))  # if '-' in result for street no
        if not street_no:
            if '/' in address_row or 'ahp' in address_row:
                street = address_row.split()[1]
            else:
                if re.search(r'\d+[A-Za-z]', address_row):  # address = house 44 , street = 44
                    street = ''.join(re.findall(r'(\d[A-Za-z]*)', address_row))  # for street no a 45b
                else:
                    result = re.search(r'\b(\d+)\b', address_row)  # adress = house 12/ house 13 , result = 12
                    if result:
                        street = result.group(1)
                    else:
                        street = ''.join(re.findall(r'\d+[A-Za-z]*', address_row))  # adress = house 45b 4 result = 45b
        else:
            street = None

        return street_no.replace('/', '') or street.replace('/', '')

    def get_type(self, response):
        return response.css('p + p::text').get('').replace('Vh.', '').strip()

    def get_size(self, response):
        return response.css('h3 + h3::text').get('').strip().replace('m²', '').replace('m', '')

    def get_agency_url(self, response):
        url = 'https://www.solidhouse.fi/'
        return urljoin(url, response.css('a::attr(href)').get(''))

    def get_static(self, response):
        url = 'https://www.solidhouse.fi/'
        return urlparse(url).netloc.split('.')[1]
