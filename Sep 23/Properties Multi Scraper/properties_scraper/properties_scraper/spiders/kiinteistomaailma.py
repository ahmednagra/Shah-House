import re

from scrapy import Request

from .base import BaseSpider


class KiinteistomaailmaSpider(BaseSpider):
    name = 'kiinteistomaailma'
    # start_urls = ['https://www.kiinteistomaailma.fi/api/km/KM/?areaType=living&limit=30&maxArea&maxYearBuilt&minArea&minYearBuilt&rental=false&sort=latestPublishTimestamp&sortOrder=desc&type=property&query[]=%7B%22district%22%3A%22Ullanlinna%22%2C%22city%22%3A%22Helsinki%22%7D&query[]=%7B%22district%22%3A%22Eira%22%2C%22city%22%3A%22Helsinki%22%7D&query[]=%7B%22district%22%3A%22Kaartinkaupunki%22%2C%22city%22%3A%22Helsinki%22%7D']

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'FEEDS': {
            f'output/properties/{name} Properties.csv': {
                'format': 'csv',
                'overwrite': True,
            }
        }
    }

    headers = {
        'authority': 'www.kiinteistomaailma.fi',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        filename = 'Kiinteistömaailma.txt'
        self.keywords_from_input_file = self.read_input_urls(filename=filename)

    def start_requests(self):
        for keyword in self.keywords_from_input_file:
            url = f'https://www.kiinteistomaailma.fi/api/km/KM/?areaType=living&limit=30&maxArea&maxYearBuilt&minArea&minYearBuilt&rental=false&sort=latestPublishTimestamp&sortOrder=desc&type=property&query[]=%7B%22district%22%3A%22{keyword}%22%2C%22city%22%3A%22Helsinki%22%7D&query[]=%7B%22district%22%3A%22Eira%22%2C%22city%22%3A%22Helsinki%22%7D&query[]=%7B%22district%22%3A%22Kaartinkaupunki%22%2C%22city%22%3A%22Helsinki%22%7D'
            yield Request(url=url, callback=self.parse, headers=self.headers)

    def parse(self, response, **kwargs):
        data = response.json().get('data', {}).get('results', [])

        for property_selector in data:
            item = self.get_item(property_selector)
            yield item

    def get_address(self, response):
        address_row = response.get('address', '')
        address = re.sub(r"\d.*", "", address_row).strip()
        return address

    def get_street_number(self, response):
        address = response.get('address', '')
        if 'Roobertinkatu' in address:
            if '-' in address:
                street_number = ''.join(re.findall(r'\d+-\d+', address))
            else:
                street_number = ''.join(re.findall(r'\d', address))
        else:
            street_number = re.sub(r'\D-\D', ' ', address.split()[1]) or re.sub(r'\d', '', address.split()[1])

        return street_number

    def get_type(self, response):
        sale_price = str(response.get('salesPriceUnencumbered', 0)).replace('€', '').split('.')[0]
        return '{:,}'.format(int(sale_price)).replace(',', ' ') + ' €'

    def get_rooms(self, response):
        rooms = response.get('roomAmount', 0)
        return rooms

    def get_size(self, response):
        return response.get('totalArea', None)

    def get_agency_url(self, response):
        return response.get('canonicalUrl', '')

    def get_static(self, response):
        return 'kiinteistomaailma'
