import json
from urllib.parse import urljoin

import scrapy
from scrapy import Request
from .base import BaseSpider


class StockxSpider(BaseSpider):
    name = "stockx"
    base_url = 'https://stockx.com/'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.use_proxy = True
        self.products = '.css-111hzm2-GridProductTileContainer'
        self.product_url = '[data-testid="productTile"] a::attr(href)'
        self.next_page = '[aria-label="Next"]::attr(href)'

        self.use_proxy = self.config.get('stocks=x', {}).get('use_proxy', '')
        self.search_urls = self.config.get('stocks=x', {}).get('search_urls', [])

    def start_requests(self):
        for url in self.search_urls:
            yield Request(url=url, callback=self.parse)

    def get_json_data(self, response):
        try:
            data = json.loads(''.join(response.css('script#__NEXT_DATA__ ::text').re(r'({.*})')))
            data = (data.get('props', {}).get('pageProps', {}).get('req', {}).get('appContext', {})
                    .get('states', {}).get('query', {}).get('value').get('queries', [{}]))
        except Exception as e:
            print(f"An error occurred: {e}")
            data = []
        product_dict = data[2].get('state', {}).get('data', {}).get('product', {})
        return product_dict

    def get_product_name(self, response, data):
        return data.get('primaryTitle', '') or response.css(
            'meta[property="twitter:title"]::attr(content)').get('')

    def get_product_brand(self, response, data):
        return data.get('brand', '')

    def get_product_category(self, response, data):
        category = data.get('category', '') or ''.join(
            [x.get('url') for x in data.get('breadcrumbs', [{}]) if x.get('level') == 2]).replace('/', '').title()
        category = category or ''.join(data.get('browseVerticals', [])).title()
        return category

    def get_retailer(self, response, data):
        logo_dict = json.loads(response.css('script[type="application/ld+json"]:contains(Organization) ::text').get(''))
        name = logo_dict.get('name', '') or 'StockX'
        url = logo_dict.get('url', '') or 'https://stockx.com/'
        logo = logo_dict.get('logo', '')
        # desc = logo_dict.get('description', '')

        content = {
            'name': name,
            'url': url,
            'logo': logo,
            # 'desc': desc
        }
        return content

    def get_retail_price(self, response, data):
        price = next((x['value'] for x in data.get('traits', []) if x.get('name') == 'Retail Price'), None)

        return price

    def get_sale_price(self, response, data):
        return 0

    def get_images(self, response, data):
        images = data.get('media', {}).get('all360Images', [])
        image_list = [x for x in images] if images else ''
        return image_list

    def get_description(self, response, data):
        des = data.get('description', '')
        des_text = '\n'.join([x.strip() for x in scrapy.Selector(text=des).xpath('//text()').getall()])

        return des_text

    def get_gender(self, response, data):
        return data.get('gender', '')

    def get_product_type(self, response, data):
        return '\n'.join(data.get('browseVerticals', []))

    def get_code(self, response, data):
        return data.get('styleId', '')

    def get_product_colorway(self, response, data):
        return next((x['value'] for x in data.get('traits', []) if x.get('name') == 'Colorway'), None)

    def get_product_main_color(self, response, data):
        colorway = self.get_product_colorway(response, data)
        main_color = response.css('[data-component="secondary-product-title"]::text').get('') or colorway.split('/')[
            0].strip()

        return main_color

    def get_release_date(self, response, data):
        return next(
            (x['value'] for x in data.get('traits', []) if x.get('name') == 'Release Date'), None)

    def get_shipping_cost(self, response, data):
        return None

    def get_coupon(self, response, data):
        return None

    def get_product_url(self, response, data):
        return urljoin(self.base_url, data.get('urlKey', ''))

    def get_variation(self, response, data):
        try:
            # Extracting JSON data from the response
            response_text = response.css('script#__NEXT_DATA__ ::text').re_first(r'({.*})')
            json_dict = json.loads(response_text)

            # Retrieving relevant data from the JSON structure
            page_props = json_dict.get('props', {}).get('pageProps', {})
            req_data = page_props.get('req', {}).get('appContext', {}).get('states', {}).get('query', {}).get('value',
                                                                                                              {})
            queries = req_data.get('queries', [{}])

            # Finding the variant data
            variants_dict = [x for x in queries if
                             'state' in x and 'data' in x['state'] and 'product' in x['state']['data'] and
                             x['state']['data']['product'].get('variants')]
            variants = variants_dict[0]['state']['data']['product']['variants']

            # Lists to store different sizes and prices
            us_male = []
            us_women = []

            for variant in variants:
                # Last Sale Price for the variation size
                # price = variant.get('market', {}).get('salesInformation', {}).get('lastSale', 0)

                # Lowest Ask Price for the variation size
                price = variant.get('market', {}).get('state', {}).get('lowestAsk', {})

                if price is not None:
                    price = price.get('amount', 0)
                else:
                    price = 0

                sizes = variant.get('sizeChart', {}).get('displayOptions', [{}])
                us_m_size = ''.join([x.get('size', '') for x in sizes if x.get('type') == 'us m'])

                if not us_m_size:
                    continue

                us_w_size = ''.join([x.get('size', '') for x in sizes if x.get('type') == 'us w'])

                if not us_w_size:
                    continue

                # Appending data to respective lists
                us_male.append({'size': us_m_size, 'price': price})
                us_women.append({'size': us_w_size, 'price': price})

            # Returning a dictionary containing all the lists
            return {
                'us_male': us_male,
                'us_women': us_women,
            }

        except Exception as e:
            # Error handling - printing the error and returning an empty dictionary
            print(f"Error in get_variation: {e}")
            return {}