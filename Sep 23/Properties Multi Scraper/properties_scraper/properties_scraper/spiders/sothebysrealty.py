import re

from scrapy import Request

from .base import BaseSpider


class SothebysrealtySpider(BaseSpider):
    name = 'sothebysrealty'
    start_urls = ['https://sothebysrealty.fi/kohteet/']

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
        'authority': 'sothebysrealty.fi',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        # 'cookie': '_ga=GA1.2.1428666450.1696940880; _gid=GA1.2.1887496911.1696940880; _fbp=fb.1.1696940882615.866551301; wp-wpml_current_language=fi; cookie_notice_accepted=true',
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

    def start_requests(self):
        yield Request(url=self.start_urls[0], headers=self.headers, callback=self.parse)

    def parse(self, response, **kwargs):
        properties_selectors = response.css('.status-publish')

        for property_selector in properties_selectors:
            item = self.get_item(property_selector)

            yield item

    def get_address(self, response):
        address = response.css('.apartment-address::text').re_first(r'(.*?)\d').strip()
        if not address:
            address = response.css('.apartment-address::text').get('').strip()

        return address

    def get_street_number(self, response):
        address_row = response.css('.apartment-address::text').get('')

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

        return street_no or street.replace('/', '')

    def get_type(self, response):
        return response.css('.apartment-sales_price::text').get('').strip()

    def get_size(self, response):
        return response.css('.apartment-total_area::text').get('').strip().replace('m²', '').replace('m', '').replace(',', '.')

    def get_agency_url(self, response):
        return response.css('.apartment-info a::attr(href)').get('')

    def get_static(self, response):
        return 'sothebys'
