import json
import os
from collections import OrderedDict
from datetime import datetime
from urllib.parse import unquote

from scrapy import Spider, Request


class PropertiesDetailsSpider(Spider):
    name = 'properties_details'
    current_dt = datetime.now().strftime("%Y-%m-%d %H%M%S")

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],

        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },

        'FEEDS': {
            f'output/Immo Scouts24 Properties Details {current_dt}.xlsx': {
                'format': 'xlsx',
                'fields': ['Rooms', 'Living Space', 'Selling Price', 'Title', 'Address', 'Geo Location',
                           'Buy Price', 'Main Information', 'Characteristics', 'Description', 'Url']
            }
        }
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        self.properties_count = 0
        self.properties_listing_count = 0
        self.properties_scraped_ids = []
        self.properties_listing_page_ids = []
        self.input_urls = self.read_input_file(file_path='input/cities_names.txt', file_type='urls')

        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'Script Started at "{self.script_starting_datetime}"\n')

        self.config = self.read_input_file(file_path='input/scrapeops_proxy_key.txt', file_type='config')
        self.proxy_key = self.config.get('scrapeops_api_key', '')
        self.use_proxy = self.config.get('use_proxy', '')

        self.properties_verification_called = False

    def start_requests(self):
        for city in self.input_urls:
            city_url = f'https://www.immoscout24.ch/en/real-estate/buy/city-{city.lower()}?r=20000'
            self.write_logs(f"Url Started for scraping : {city_url}")

            yield Request(url=city_url, headers=self.headers, callback=self.parse_city_filters,
                          meta={'city_url': city_url, 'city_name': city})

    def parse_city_filters(self, response):
        response_url = ''.join(unquote(response.url).split('url=')[1:2])
        data = self.get_json_data(response, response_url)

        try:
            search_data = data.get('resultList', {}).get('search', {}).get('fullSearch', {}).get('result', {})
            info_data = search_data if search_data is not None else {}

            total_properties = info_data.get('resultCount', 0)

            if total_properties == 0 or not total_properties:
                self.write_logs(f"No Property Exist in URl:{response_url}\n")
                return

            # Rooms No filter
            if total_properties >= 1000 and '&nrf=' not in unquote(response.url):
                self.write_logs(f'Rooms Filter called : Total Properties {total_properties} Url:{response_url} \n')
                self.write_logs(
                    f"Total Properties : {total_properties} Filter :'''{response.meta.get('filter_name', '')}''' URl:{response_url}")
                for rooms in range(0, 13):
                    url = f"{''.join(unquote(response.url).split('url=')[1:])}&nrf={rooms}&nrt={int(rooms) + 1}"
                    response.meta['price_filter_url'] = url
                    yield Request(url=url, callback=self.parse_city_filters, meta=response.meta)
                return

            # Space Living Filter
            if total_properties >= 1000 and '&nrf=' in unquote(response.url) and '&slf=' not in unquote(response.url):
                self.write_logs(
                    f' Space Living Filter called : Total Properties {total_properties} Url:{response_url} \n')
                living_space_filter = ['0', '20', '40', '60', '80', '100', '120', '140', '160', '180',
                                       '200', '250', '300', '350', '400', '450', '500', '550', '80000', '80000000',
                                       '80000000000000000']
                # Loop through the living_space_filter list
                for i in range(len(living_space_filter) - 1):
                    slf_value = living_space_filter[i]
                    slt_value = living_space_filter[i + 1]

                    url = f"{''.join(unquote(response.url).split('url=')[1:])}&slf={slf_value}&slt={slt_value}"
                    response.meta['space_filter_url'] = url
                    yield Request(url=url, callback=self.parse_city_filters, meta=response.meta)
                return

            total_pages = info_data.get('pageCount', 0)
            current_page = info_data.get('page', 0)

            if current_page == 1 and total_properties <= 1000:
                self.write_logs(
                    f"City : {response.meta.get('city_name', '')} Filter :{response.meta.get('filter_name', '')}  Has Total Properties {total_properties}  Total pagination Page :{total_pages} Response Url :{''.join(unquote(response.url).split('url=')[1:])} \n\n")

            properties_per_page = info_data.get('itemsPerPage', 0)
            self.properties_listing_count += int(properties_per_page )

            # Property Detail Page Requests
            properties = info_data.get('listings', [])
            for property_dict in properties[:1]:
                property_id = property_dict.get('id', '')

                # duplicate Property_Id Check
                if property_id in self.properties_listing_page_ids:
                    print('Property Id Already scraped :', property_id)
                    continue

                title = property_dict.get('listing', {}).get('localization', {}).get('de', {}).get('text', {}).get(
                    'title', '')
                url = f'https://www.immoscout24.ch/buy/{property_id}'
                if not url:
                    self.write_logs(f'Property: {title} not found ID\n')
                    return

                self.properties_listing_page_ids.append(property_id)
                yield Request(url=url, callback=self.parse_property_detail)
        except Exception as e:
            self.write_logs(f"URl:{response_url} Error Parsing Listing Page Error: {e}")

        try:
            # Pagination
            next_page = response.css(
                '[class*="HgPaginationSelector_nextPreviousArrow"][aria-label="Go to next page"] ::attr(href)').get('')
            if next_page:
                url = f'https://www.immoscout24.ch{next_page}'
                next_page_value = ''.join(''.join(next_page.split('pn=')[1:2]).split('&r=')[0:1])
                print(f'Requesting page number: ""{next_page_value}"" requested')
                yield Request(url=url, callback=self.parse_city_filters, meta=response.meta)
        except Exception as e:
            self.write_logs(f"Error requesting next page at URL: {response.meta.get('filter_url', '')}. Error: {e}")

    def parse_property_detail(self, response):
        data = self.get_json_data(response=response, response_url=None)
        title = response.css('[class*="ListingTitle_spotlightTitle_"]::text').get('')

        try:
            property_dict = data.get('listing', {}).get('listing', {})
            living_space = property_dict.get('characteristics', {}).get('livingSpace', 0)
            room = property_dict.get('characteristics', {}).get('numberOfRooms', 0.0) or response.css('[class*="SpotlightAttributesNumberOfRooms_value_"] ::text').get('').strip()
            selling_price = ' '.join(response.css('.SpotlightAttributesPrice_value_TqKGz span ::text').getall())
            buying_price = ' '.join(response.css('[data-test="costs"] dd ::text').getall())
            property_id = property_dict.get('id', '')

            item = OrderedDict()
            item['Rooms'] = room if room != 0 else ''
            item['Living Space'] = f'{living_space} m²' if living_space else ''
            item['Selling Price'] = selling_price.replace('.–', '').strip() if selling_price else ''
            item['Title'] = title.replace('"', ' ') if title else ''
            item['Address'] = ''.join(response.css('[class*="AddressDetails_address_"] ::text').getall())
            item['Geo Location'] = self.get_geolocation(property_dict)
            item['Buy Price'] = buying_price.replace('.–', '').strip() if buying_price else ''
            item['Main Information'] = self.get_information(response)
            item['Characteristics'] = ', '.join(response.css('[class*="FeaturesFurnishings_list_"] ::text').getall())
            item['Description'] = ' '.join([value.strip() for value in response.css('[class*="Description_descriptionBody_"] ::text').getall()])
            item['Url'] = response.css('meta[property="og:url"] ::attr(content)').get('')

            self.properties_count += 1
            print('properties_count : ', self.properties_count)
            self.properties_scraped_ids.append(property_id)
            yield item

        except Exception as e:
            self.write_logs(f'Property {title} Not yield Error: {e}')

    def get_information(self, response):
        """
        Extract key-value pairs of property information from the response.
        If any error occurs, log the error with the property ID.
        """
        property_id = ''
        url = response.css('meta[property="og:url"] ::attr(content)').get('')
        if url:
            property_id = url.split('/')[-1]

        try:
            # Extract key-value pairs of property information
            dt_elements = response.css('[class*="CoreAttributes_coreAttributes_"] dl dt')
            dd_elements = response.css('[class*="CoreAttributes_coreAttributes_"] dl dd')
            attributes = []

            # Iterate over dt and dd pairs and create attribute strings
            for dt, dd in zip(dt_elements, dd_elements):
                key = dt.xpath('string()').get('').strip()
                value = dd.xpath('string()').get('').strip()
                if key:
                    attributes.append(f"{key} {value}\n")

            attributes_string = ''.join(attributes)
            return attributes_string
        except Exception as e:
            self.write_logs(f"ID:{property_id} Parsing error for Information. Error: {e}")
            return ''

    def get_geolocation(self, data):
        try:
            # Convert latitude and longitude to degrees, minutes, and seconds format
            geo_dict = data.get('address', {}).get('geoCoordinates', {})
            latitude = geo_dict.get('latitude', 0.0)
            longitude = geo_dict.get('longitude', 0.0)

            def decimal_degrees_to_dms(decimal_degrees):
                degrees = int(decimal_degrees)
                minutes_decimal = (decimal_degrees - degrees) * 60
                minutes = int(minutes_decimal)
                seconds = (minutes_decimal - minutes) * 60
                return f"{degrees}°{minutes:02d}'{seconds:.1f}\""

            latitude_dms = decimal_degrees_to_dms(latitude)
            longitude_dms = decimal_degrees_to_dms(longitude)

            latitude_direction = 'N' if latitude >= 0 else 'S'
            longitude_direction = 'E' if longitude >= 0 else 'W'

            return f"{latitude_dms}{latitude_direction} {longitude_dms}{longitude_direction}"

        except Exception as e:
            self.write_logs(f"ID: {data.get('id', '')} Parsing error for Geo Location Error: {e}")

    def get_json_data(self, response, response_url):
        url = response_url or ''.join(unquote(response.url).split('url=')[1:])
        try:
            data = json.loads(
                response.css('script:contains("__INITIAL_STATE__")::text').re_first(r'window.__INITIAL_STATE__=(.*)'))
        except json.JSONDecodeError as e:
            data = {}
            self.write_logs(f"Url :{url}   JSON Parse error: {e} \n")

        return data

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def read_input_file(self, file_path, file_type):
        """
        Read URLs or configuration from a text file.
        """
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                if file_type == 'urls':
                    return [line.strip() for line in file.readlines() if line.strip()]
                elif file_type == 'config':
                    return {line.split('==')[0].strip(): line.split('==')[1].strip() for line in file if '==' in line}
        except FileNotFoundError:
            self.write_logs(f"File not found: {file_path}")
            return [] if file_type == 'urls' else {}
        except Exception as e:
            self.write_logs(f"An error occurred while reading {file_type} file: {str(e)}")
            return [] if file_type == 'urls' else {}

    def properties_verification(self):
        """ check the Properties Requested from listing page is scraped successfully or not.
        if not then again make request to scraped"""
        not_scraped_ids = [pid for pid in self.properties_listing_page_ids if pid not in self.properties_scraped_ids]
        self.write_logs(f" Properties scraped from Properties Verification Function: {not_scraped_ids} ")
        for pid in not_scraped_ids:
            url = f'https://www.immoscout24.ch/buy/{pid}'
            yield Request(url=url, callback=self.parse_property_detail)

    def close(spider, reason):
        if not spider.properties_verification_called:
            spider.properties_verification_called = True
            spider.properties_verification()
        spider.write_logs(f'Total Properties from listing page : {spider.properties_listing_count}')
        spider.write_logs(f'Total Properties Scraped : {spider.properties_count}')
