import json
from collections import OrderedDict
from datetime import datetime
from urllib.parse import urljoin
import psycopg2
import psycopg2.pool
from scrapy import Spider, Request


class BaseSpider(Spider):
    name = 'base'
    base_url = ''
    output_filename = ''

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.proxy = ''

        # Selectors
        self.product_url = ''
        self.products = ''
        self.new_price = ''
        self.next_page = ''

        self.output_filename = f'output/{self.name} Products.csv'

        self.output_fieldnames = ['Product_Name', 'Product_Brand', 'Product_Retail_Price', 'Product_Sale_Price',
                                  'Product_Image', 'Description', 'Gender', 'Product_Type', 'Product_Code',
                                  'Colorway', 'Main_Color', 'Release_Date', 'Shipping_Cost', 'Coupon', 'variations',
                                  'Product_URL', 'Retailer_id', 'Retailer_logo', 'Retailer_logo']

        self.config = self.read_config_file()

        database_name = self.config.get('db_name')
        self.connection = self.get_connection_database(database_name)
        self.cursor = self.connection.cursor()

        self.get_all_tables()

        self.proxy_key = self.config.get('scrapeops_api_key', '')
        self.use_proxy = False

    def parse(self, response, **kwargs):
        products = response.css(f'{self.products}')

        for product in products[:2]:
            product_url = product.css(f'{self.product_url}').get('').rstrip('/').strip()
            if not product_url:
                continue

            product_url = urljoin(self.base_url, product_url)

            yield Request(url=product_url, callback=self.product_detail)

        if self.next_page:
            next_page = response.css(f'{self.next_page}').get('')

            if next_page:
                next_url = urljoin(self.base_url, next_page)
                # yield Request(url=next_url, callback=self.parse)

    def product_detail(self, response):
        data = self.get_json_data(response)
        item = self.get_item(response, data)

    def get_item(self, response, data):
        item = OrderedDict()

        date = self.get_release_date(response, data) or ''
        if isinstance(date, str):
            try:
                # Convert the string representation of the date into a Python date object
                release_date_obj = datetime.strptime(date, '%m/%d/%y')

                # Use strftime to format the date as 'YYYY-MM-DD'
                release_date = release_date_obj.strftime('%Y-%m-%d')
            except ValueError:
                # If the conversion fails, assume it's already in the correct format
                release_date = date
        else:
            # If it's not a string, assume it's already in the correct format
            release_date = date

        item['Product_Name'] = self.get_product_name(response, data) or ''
        item['Product_Brand'] = self.get_product_brand(response, data) or ''
        item['Product_Category'] = self.get_product_category(response, data) or ''
        item['Product_Retail_Price'] = self.get_retail_price(response, data) or 0
        item['Product_Sale_Price'] = self.get_sale_price(response, data) or 0
        item['Product_Image'] = self.get_images(response, data) or []
        item['Description'] = self.get_description(response, data) or ''
        item['Gender'] = self.get_gender(response, data) or ''
        item['Product_Type'] = self.get_product_type(response, data) or ''
        item['Product_Code'] = self.get_code(response, data) or ''
        item['Colorway'] = self.get_product_colorway(response, data) or ''
        item['Main_Color'] = self.get_product_main_color(response, data) or ''
        item['Release_Date'] = release_date
        item['Shipping_Cost'] = self.get_shipping_cost(response, data) or ''
        item['Coupon'] = self.get_coupon(response, data) or ''
        item['variations'] = self.get_variation(response, data) or ''
        item['Product_URL'] = self.get_product_url(response, data) or ''
        item['Retailer_info'] = self.get_retailer(response, data) or ''

        self.insert_into_database(item)

        return item

    def read_config_file(self):
        file_path = 'input/config.json'
        config = {}

        try:
            with open(file_path, mode='r') as json_file:
                data = json.load(json_file)
                config.update(data)

            return config

        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {str(e)}")
            return {}
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return {}

    def get_connection_database(self, database_name):
        try:
            # Connection parameters
            db_params = {
                'user': self.config.get('db_user', ''),
                'password': self.config.get('db_password', ''),
                'host': self.config.get('db_host', ''),
                'port': self.config.get('db_port', ''),
                'dbname': '',  # Connect to the default 'postgres' database initially
            }

            # Connect to the default 'postgres' database
            conn = psycopg2.connect(**db_params)
            conn.autocommit = True

            # Check if the target database exists
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            database_exists = cursor.fetchone() is not None

            if database_exists:
                print(f'Database {database_name} already exist')

            if not database_exists:
                # Create the target database
                cursor.execute(f'CREATE DATABASE "{database_name}"')
                print(f'Database {database_name} created Successfully')

            # Close the connection to the default 'postgres' database
            conn.close()

            # Connect to the target database
            db_params['dbname'] = database_name
            updated_conn = psycopg2.connect(**db_params)
            updated_conn.autocommit = True

            return updated_conn

        except psycopg2.Error as e:
            print(f"Error: {e}")
            raise

    def create_table(self, table_name, columns):
        try:
            # cursor = self.db_connection.cursor()

            # Check if the table exists
            self.cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_catalog = %s;
            """, (self.config.get('db_name'),))

            tables = self.cursor.fetchall()
            tables_list = [table[0] for table in tables]

            if table_name not in tables_list:
                create_table_query = f'''
                    CREATE TABLE "{table_name}" (
                        {columns}
                    );
                '''

                # Execute the SQL query to create the table
                self.cursor.execute(create_table_query)
                print(f"Table '{table_name}' created successfully.")

            else:
                print(f"Table '{table_name}' already exists.")

            self.connection.commit()

        except psycopg2.Error as e:
            print(f"Error creating/checking table: {e}")
            raise

    # create table definition in list for all tables
    def get_all_tables(self):
        # Define table information
        # table are ordered because the usage of foreign key
        table_info = [
            ('products_brand', '''
            id BIGSERIAL PRIMARY KEY,
            name CHARACTER VARYING(100) UNIQUE NOT NULL,
            logo CHARACTER VARYING(100),
            description TEXT
        '''),
            ('products_category', '''
                id BIGSERIAL PRIMARY KEY,
                name character varying(100) UNIQUE NOT NULL
            '''),
            ('products_product', '''
                id BIGSERIAL PRIMARY KEY,
                name CHARACTER VARYING(255) NOT NULL,
                description TEXT,
                release_date DATE,
                product_code CHARACTER VARYING(255) UNIQUE NOT NULL,
                colorway CHARACTER VARYING(255),
                main_color CHARACTER VARYING(255),
                brand_id BIGINT,
                category_id BIGINT,
                sale_price NUMERIC(10,2),
                product_url CHARACTER VARYING(500),
                published_date TIMESTAMP,
                FOREIGN KEY (brand_id) REFERENCES products_brand (id),
                FOREIGN KEY (category_id) REFERENCES products_category (id)
            '''),
            ('products_retailer', '''
                       id BIGSERIAL PRIMARY KEY,
                       name CHARACTER VARYING(255) UNIQUE NOT NULL,
                       url CHARACTER VARYING(500) NOT NULL,
                       affiliate_program_url CHARACTER VARYING(500),
                       logo CHARACTER VARYING(100)
                   '''),
            # retailer id, product_id pe unique constraints
            ('products_productavailability', '''
                        id BIGSERIAL PRIMARY KEY,
                        price NUMERIC(10,2),
                        shipping_cost NUMERIC(10,2),
                        last_checked TIMESTAMP,
                        product_id BIGINT NOT NULL,
                        retailer_id BIGINT NOT NULL,
                        FOREIGN KEY (product_id) REFERENCES products_product (id),
                        FOREIGN KEY (retailer_id) REFERENCES products_retailer (id),
                        CONSTRAINT unique_product_retailer_combination UNIQUE (product_id, retailer_id)
                    '''),
            ('products_size', '''
                        id BIGSERIAL PRIMARY KEY,
                        value CHARACTER VARYING(50) UNIQUE NOT NULL
                    '''),
            # retailer id, product_id, product_size pe unique constraints
            ('price_productprice', '''
                id BIGSERIAL PRIMARY KEY,
                price NUMERIC(10,2) NOT NULL,
                shipping_cost NUMERIC(10,2),
                last_checked TIMESTAMP,
                product_id BIGINT NOT NULL,
                retailer_id BIGINT,
                size_id BIGINT,
                FOREIGN KEY (product_id) REFERENCES products_product (id),
                FOREIGN KEY (retailer_id) REFERENCES products_retailer (id),
                FOREIGN KEY (size_id) REFERENCES products_size (id),
                CONSTRAINT unique_product_retailer_size_combination UNIQUE (product_id, retailer_id,size_id)
            '''),
            # products_productavailability id, products_size_id pe unique constraints
            ('products_productavailability_sizes', '''
                id BIGSERIAL PRIMARY KEY,
                productavailability_id BIGINT NOT NULL,
                size_id BIGINT NOT NULL,
                FOREIGN KEY (productavailability_id) REFERENCES products_productavailability (id),
                FOREIGN KEY (size_id) REFERENCES products_size (id),
                CONSTRAINT unique_productavailability_size_combination UNIQUE (productavailability_id, size_id)
            '''),
            # products_productavailability id, products_size_id pe unique constraints
            ('products_sizeprice', '''
                        id BIGSERIAL PRIMARY KEY,
                        price NUMERIC(10,2) NOT NULL,
                        product_availability_id BIGINT NOT NULL,
                        size_id BIGINT NOT NULL,
                        FOREIGN KEY (product_availability_id) REFERENCES products_productavailability (id),
                        FOREIGN KEY (size_id) REFERENCES products_size (id),
                        CONSTRAINT unique_products_sizeprice_availability_size_combination UNIQUE (product_availability_id, size_id)
                    '''),
            # products_product id, image_url pe unique constraints
            ('products_productimage', '''
                        id BIGSERIAL PRIMARY KEY,
                        image_url CHARACTER VARYING(10000) NOT NULL,
                        product_id bigint NOT NULL,
                        FOREIGN KEY (product_id) REFERENCES products_product (id),
                        CONSTRAINT unique_products_image_combination UNIQUE (product_id, image_url)
            ''')
        ]

        for table_name, columns in table_info:
            self.create_table(table_name, columns)

    def insert_into_database(self, item):
        cursor = None  # Initialize cursor outside the try block
        try:
            # Insert into products_brand table
            brand_id = self.insert_products_brand(item)

            # insert into Category Table
            category_id = self.insert_product_category(item)

            # Insert into products_product table
            product_id = self.insert_products_product(item, brand_id, category_id)

            # Insert into products_productimage table
            self.insert_products_productimage(item, product_id)

            # Insert into Product_retailer
            retailer_id = self.insert_products_retailer(item)

            cost = item.get('Shipping_Cost')
            shipping_cost = cost if cost else 0.0
            # last_checked = datetime.now()
            last_checked = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Insert into products_productavailability
            products_productavailability_id = self.insert_products_productavailability(item, shipping_cost,
                                                                                       last_checked, product_id,
                                                                                       retailer_id)

            # Insert into products_size
            for size_country, size_list in item.get('variations', {}).items():
                for size_entry in size_list:
                    # Insert into products_size
                    products_size_id = self.insert_products_size(size_entry)

                    # Insert into products_productavailability_sizes
                    self.insert_products_productavailability_sizes(products_productavailability_id,
                                                                   products_size_id)

                    # Insert into products_size and products_sizeprice tables
                    price = size_entry.get('price', '')
                    self.insert_products_sizeprice(price,
                                                   products_productavailability_id,
                                                   products_size_id)

                    # Insert into price_productprice
                    self.insert_price_productprice(price, product_id, shipping_cost,
                                                   last_checked, retailer_id, products_size_id)

            # Commit changes only if everything is successful
            self.connection.commit()

        except psycopg2.Error as e:
            print(f"Error inserting data into the database: {e}")
            # Rollback changes if an exception occurs
            self.connection.rollback()
            raise

        finally:
            if cursor:
                self.cursor.close()

    def get_json_data(self, response):
        pass

    def get_product_name(self, response, data):
        pass

    def get_product_brand(self, response, data):
        pass

    def get_product_category(self, response, data):
        pass

    def get_retail_price(self, response, data):
        pass

    def get_sale_price(self, response, data):
        pass

    def get_images(self, response, data):
        pass

    def get_description(self, response, data):
        pass

    def get_gender(self, response, data):
        pass

    def get_product_type(self, response, data):
        pass

    def get_code(self, response, data):
        pass

    def get_product_colorway(self, response, data):
        pass

    def get_product_main_color(self, response, data):
        pass

    def get_release_date(self, response, data):
        pass

    def get_shipping_cost(self, response, data):
        pass

    def get_coupon(self, response, data):
        pass

    def get_product_url(self, response, data):
        pass

    def get_variation(self, response, data):
        pass

    def get_retailer(self, response, data):
        pass

    def insert_products_brand(self, item):
        """
        Insert data into the products_brand table.

        Args:
            cursor: The database cursor.
            item: The data item containing information about the brand.

        Returns:
            int: The ID of the inserted brand.
        """
        try:
            brand_name = item.get('Product_Brand').strip().title()

            table_name = 'products_brand'
            columns = ['name']
            values = (brand_name,)
            unique_column_name = 'name'

            bid = self.insert_single_item(table_name, columns, values, unique_column_name)
            return bid

        except psycopg2.Error as e:
            print(f"Error inserting data into products_brand: {e}")
            raise

    def insert_product_category(self, item):
        try:
            category_name = item.get('Product_Category', '').strip().title()
            table_name = 'products_category'
            columns = ['name']
            values = (category_name,)
            unique_column_name = 'name'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_category: {e}")
            raise

    def insert_products_product(self, item, brand_id, category_id):
        """
        Insert data into the products_product table.

        Args:
            cursor: The database cursor.
            item: The data item containing information about the product.
            brand_id: The ID of the brand associated with the product.

        Returns:
            int: The ID of the inserted product, or None if no row was inserted.
        """
        try:
            product_name = item.get('Product_Name', '')
            description = item.get('Description', None)
            release_date = item.get('Release_Date', '') if item.get('Release_Date', '') else None
            product_code = item.get('Product_Code', None)
            colorway = item.get('Colorway', '')
            main_color = item.get('Main_Color', '')
            product_url = item.get('Product_URL', '')
            published_date = item.get('Published_Date', None)
            sale_price = item.get('Sale_Price', '')

            table_name = 'products_product'
            columns = ['name', 'description', 'release_date', 'product_code', 'colorway', 'main_color', 'brand_id',
                       'sale_price', 'category_id', 'product_url', 'published_date']
            values = (product_name, description, release_date, product_code, colorway, main_color,
                      brand_id, sale_price, category_id, product_url, published_date)
            unique_column_name = 'product_code'

            product_id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return product_id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_product: {e}")
            raise

    def insert_products_productimage(self, item, product_id):
        """
        Insert data into the products_productimage table.

        Args:
            cursor: The database cursor.
            item: The data item containing information about the product.
            product_id: The ID of the product associated with the image.

        Returns:
            list: List of IDs for the inserted images.
        """
        image_ids = []

        try:
            images = item.get('Product_Image', [])
            for image_url in images:
                table_name = 'products_productimage'
                columns = ['image_url', 'product_id']
                values = (image_url, product_id)
                unique_column_names = 'image_url, product_id'

                id = self.insert_single_item(table_name, columns, values, unique_column_names)
                image_ids.append(id)

        except psycopg2.Error as e:
            print(f"Error inserting data into products_productimage: {e}")
            raise

        return image_ids

    def insert_products_retailer(self, item):
        """
        Insert data into the products_retailer table.

        Args:
            cursor: The database cursor.
            item: The data item containing information about the retailer.

        Returns:
            int: The ID of the inserted retailer.
        """
        try:
            retailer_name = item.get('Retailer_info', {}).get('name', '')
            retailer_url = item.get('Retailer_info', {}).get('url', '')
            retailer_logo = item.get('Retailer_info', {}).get('logo', '')
            table_name = 'products_retailer'
            columns = ['name', 'url', 'logo']
            values = (retailer_name, retailer_url, retailer_logo)
            unique_column_name = 'name'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_retailer: {e}")
            raise

    def insert_products_productavailability(self, item, shipping_cost, last_checked, product_id, retailer_id):
        """
        Insert data into the products_productavailability table.

        Args:
            cursor: The database cursor.
            item: The data item containing information about product availability.
            shipping_cost: The shipping cost.
            last_checked: The timestamp of the last check.
            product_id: The ID of the product.
            retailer_id: The ID of the retailer.

        Returns:
            int: The ID of the inserted product availability.
        """
        try:
            retail_price = item.get('Product_Retail_Price', 0.0)
            table_name = 'products_productavailability'
            columns = ['price', 'shipping_cost', 'last_checked', 'product_id', 'retailer_id']
            values = (retail_price, shipping_cost, last_checked, product_id, retailer_id)
            unique_column_name = 'product_id, retailer_id'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_productavailability: {e}")
            raise

    def insert_products_size(self, size_entry):
        """
        Insert data into the products_size table.

        Args:
            cursor: The database cursor.
            size_entry: The data item containing information about the size.

        Returns:
            int: The ID of the inserted size.
        """
        try:
            size = size_entry.get('size', '').strip()
            table_name = 'products_size'
            columns = ['value']
            values = (size,)
            unique_column_name = 'value'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_size: {e}")
            raise

    def insert_products_productavailability_sizes(self, products_productavailability_id, products_size_id):
        """
        Insert data into the products_productavailability_sizes table.

        Args:
            cursor: The database cursor.
            products_productavailability_id: The ID of the product availability.
            products_size_id: The ID of the size.

        Returns:
            int: The ID of the inserted product availability size.
        """
        try:
            table_name = 'products_productavailability_sizes'
            columns = ['productavailability_id', 'size_id']
            values = (products_productavailability_id, products_size_id)
            unique_column_name = 'productavailability_id, size_id'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_productavailability_sizes: {e}")
            raise

    def insert_products_sizeprice(self, price, products_productavailability_id, products_size_id):
        """
        Insert data into the products_sizeprice table.

        Args:
            cursor: The database cursor.
            price: The price of the size.
            products_productavailability_id: The ID of the product availability.
            products_size_id: The ID of the size.

        Returns:
            int: The ID of the inserted size price.
        """

        try:
            table_name = 'products_sizeprice'
            columns = ['price', 'product_availability_id', 'size_id']
            values = (price, products_productavailability_id, products_size_id)
            unique_column_name = 'product_availability_id, size_id'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into products_sizeprice: {e}")
            raise

    def insert_price_productprice(self, price, product_id, shipping_cost, last_checked, retailer_id,
                                  products_size_id):
        """
        Insert data into the price_productprice table.

        Args:
            cursor: The database cursor.
            price: The price of the product.
            product_id: The ID of the product.
            shipping_cost: The shipping cost.
            last_checked: The timestamp of the last check.
            retailer_id: The ID of the retailer.
            products_size_id: The ID of the size.

        Returns:
            int: The ID of the inserted price product price.
        """

        try:
            table_name = 'price_productprice'
            columns = ['price', 'shipping_cost', 'last_checked', 'product_id', 'retailer_id', 'size_id']
            values = (price, shipping_cost, last_checked, product_id, retailer_id, products_size_id)
            unique_column_name = 'product_id, retailer_id, size_id'

            id = self.insert_single_item(table_name, columns, values, unique_column_name)
            return id

        except psycopg2.Error as e:
            print(f"Error inserting data into price_productprice: {e}")
            raise

    def insert_single_item(self, table_name: str, columns: list, values: tuple, unique_column_names):
        try:
            # Check if the connection is closed and reconnect if needed
            if self.connection.closed != 0:
                self.connection = self.get_connection_database(self.config.get('db_name'))
                self.cursor = self.connection.cursor()

            table_fields = ', '.join(columns)
            place_holders = ', '.join(['%s'] * len(columns))
            update_fields = ',\n '.join([f'{col} = EXCLUDED.{col}' for col in columns])

            self.cursor.execute(f"""
                        INSERT INTO {table_name} ({table_fields})
                        VALUES ({place_holders})
                        ON CONFLICT ({unique_column_names}) DO UPDATE
                        SET {update_fields}
                        RETURNING id;
                    """, values)

            row = self.cursor.fetchone()

            if row:
                id = row[0]
                print(f'{table_name}_id:', id)

                # reset the primary key at last id +1 in the table
                self.reset_primary_key(table_name)
                return id

        except psycopg2.Error as e:
            print(f"Error inserting data into {table_name}: {e}")
            raise

    def reset_primary_key(self, table_name: str):
        try:
            # Reset the auto-increment counter for the products_brand_id_seq sequence
            # reset the primary key at last id +1 in table
            self.cursor.execute(
                f"SELECT setval('{table_name}_id_seq', (SELECT COALESCE(MAX(id), 0) FROM {table_name}) + 1, false);")
        except psycopg2.Error as e:
            print(f"Error resetting primary key for {table_name}: {e}")
            raise
