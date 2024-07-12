import re
import requests

from scrapy import Request

from .base import BaseSpider


class HabitaSpider(BaseSpider):
    name = 'habita'
    # start_urls = ['https://www.habita.com']

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
        filename = 'habita.txt'
        self.keywords_from_input_file = self.read_input_urls(filename=filename)

    def start_requests(self):
        for keyword in self.keywords_from_input_file:
            keyword_ids = self.get_keyword_id(keyword)

            if keyword_ids:
                for id in keyword_ids:
                    url = f'https://www.habita.com/propertysearch/results/fi/1/28/full?districts={id}&sort=newest&type=ResidenceSale'
                    yield Request(url=url, callback=self.parse)
            else:
                print('No search City Keyword found in response ')

    def parse(self, response, **kwargs):
        try:
            data = response.json()
            results = data.get('results', [])
        except Exception as e:
            results = []
            return

        for row in results:
            property_id = row.get('id', '')
            priced = row.get('price', 0)
            price = str(priced)
            size = row.get('area', '').replace('m²', '').strip()
            property_url = f'https://www.habita.com/kohde/{property_id}'
            yield Request(url=property_url, callback=self.parse_property_detail, meta={'price': price, 'size': size})

    def parse_property_detail(self, response):
        item = self.get_item(response)
        yield item

    def get_address(self, response):
        address_row = response.css('#descriptions h1::text').get('').replace('Kerrostalo,', '').strip()
        # return ' '.join(re.findall('[a-zA-Z]+', address_row))
        match = re.search(r'^(.*?)\d', address_row)
        if match:
            address = match.group(1)
        else:
            address = address_row

        return address

    def get_street_number(self, response):
        address = response.css('#descriptions h1::text').get('')

        if '-' in address:
            # street_no = re.sub(r'\D-\D', '', address)
            street_no = re.findall(r'(\d+-\d+)', address)[0]
        else:
            street_no = ''.join(re.findall(r'(\d+\D*)', address)).split(' ')[0]
            # street_no = ''.join(re.findall(r'\d', address))
        return street_no

    def get_type(self, response):
        sale_price = response.meta.get('price', '')
        price = int(sale_price)
        formatted_price = "{:,}".format(price).replace(',', ' ')
        return formatted_price + ' €'

    def get_rooms(self, response):
        rooms = response.css('#key-details  div + div span::text').get('')
        return rooms

    def get_size(self, response):
        return response.meta.get('size', '')

    def get_agency_url(self, response):
        return response.url

    def get_static(self, response):
        return 'habita'

    def get_keyword_id(self, keyword):
        res = requests.get(url=f'https://www.habita.com/location/suggestions/fi/{keyword}')
        try:
            data = res.json()
            id = [x.get('id', '') for x in data if x.get('type', '') == 'district']
        except Exception as e:
            id = []

        return id

    def get_city_results(self, id):
        url = f'https://www.habita.com/propertysearch/results/fi/1/28/full?districts={id}&sort=newest&type=ResidenceSale'
        res = requests.get(url=url)
        try:
            data = res.json()
            results = data.get('results', [])
            ids = [x.get('id', '') for x in results]
        except Exception as e:
            ids = []

        return ids
