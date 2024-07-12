import json

import scrapy
from scrapy import Request
from .base import BaseSpider
from urllib.parse import urljoin


class SneakerstuffSpider(BaseSpider):
    name = "sneakerstuff"
    base_url = 'https://www.sneakersnstuff.com/en'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.use_proxy = self.config.get('sneakersnstuff', {}).get('use_proxy', '')
        self.products = '.card.product'
        self.product_url = '.card__link ::attr(href)'
        self.new_price = '.price__current::text'
        self.next_page = '.pagination__next ::attr(href)'

        self.search_urls = self.config.get('sneakersnstuff', {}).get('search_urls', [])

    def start_requests(self):
        for url in self.search_urls:
            yield Request(url=url, callback=self.parse)

    def get_json_data(self, response):
        try:
            data = json.loads(response.css('#product-data::text').get(''))
        except Exception as e:
            print(f"An error occurred: {e}")
            data = []

        return data

    def get_product_name(self, response, data):
        return data.get('name', '')

    def get_product_brand(self, response, data):
        return data.get('brand', '')

    def get_product_category(self, response, data):
        category = data.get('category', '') or data.get('subBrand', '')
        category = category or ''.join(response.css('.breadcrumb__link span::text').getall()[1:2]).title()
        return category

    def get_retailer(self, response, data):
        name = 'Sneakersnstuff'
        url = response.css('meta[property="og:site_name"] ::attr(content)').get('') or 'https://www.sneakersnstuff.com/en/'
        logo = ''
        content = {
            'name': name,
            'url': url,
            'logo': logo
        }
        return content

    def get_retail_price(self, response, data):
        return str(data.get('priceOriginalCurrency', 0))

    def get_sale_price(self, response, data):
        return 0

    def get_images(self, response, data):
        images = response.css('.image-gallery__track a::attr(href)').getall()
        return [urljoin(self.base_url, x) for x in images] if images else ''

    def get_description(self, response, data):
        try:
            description = '\n'.join([x.strip() for x in scrapy.Selector(text=data.get('description', '')).xpath('//text()').getall()]).replace(' ', ' ')
            desc = response.css('.product-view__description p::text').getall() or []
            des1 = '\n'.join([x.replace(' ', ' ') for x in desc]) if desc else ''
            des = description or des1

            return des
        except ValueError as e:
            print(f"Unable to get Description due to : {e}")
            return ''

    def get_gender(self, response, data):
        return data.get('gender', '')

    def get_product_type(self, response, data):
        return None

    def get_code(self, response, data):
        return data.get('displayId', '')

    def get_product_colorway(self, response, data):
        return data.get('supplierColor', '')

    def get_product_main_color(self, response, data):
        colorway = self.get_product_colorway(response, data)
        main_color = data.get('color', '').strip() or colorway.split('/')[0].strip()

        return main_color

    def get_release_date(self, response, data):
        return data.get('release', '')

    def get_shipping_cost(self, response, data):
        return None

    def get_coupon(self, response, data):
        return None

    def get_product_url(self, response, data):
        return data.get('url', '')

    def get_variation(self, response, data):
        # Selectors for product sizes
        selectors = response.css('#product-size option')
        price = response.css('.product-view__price .price[data-currency="USD"] ::attr(data-value)').get('')
        # Initialize dictionaries to store sizes for each country
        variations = {
            'us': [],
        }

        # Iterate over each selector
        for selector in selectors:
            # Extract data-size-types attribute
            string = selector.css('option::attr(data-size-types)').get('')

            # Skip if data-size-types is not present
            if not string:
                continue

            # Parse JSON data
            data = json.loads(string)

            # Extract value attribute
            value = selector.css('option::attr(value)').get('')

            if not value:
                continue

            # Extract sizes for each country
            us_size = data.get('converted-size-size-us')

            if not us_size:
                continue

            # Append size information to respective lists in the variations dictionary
            variations['us'].append({'size': us_size, 'value': value, 'price': price})

        # Return the variations dictionary
        return variations

