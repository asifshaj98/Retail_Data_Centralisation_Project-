import re
import pandas as pd
import numpy as np
class DataCleaning:
    def clean_user_data(self, user_df):
        
    
        return user_df
    
    def clean_card_data(self, card_df):
        '''
         Cleans a DataFrame containing credit card data by performing the following steps:
    
        1. Replaces null values with 'Unknown'.
        2. Drops rows with null values.
        3. Removes leading question marks from card numbers.
        4. Removes non-numeric characters from card numbers.
        5. Converts the date column to a datetime object.
        6. Converts the card number column to an integer.
        7. Converts the card provider column to a category.
        '''
        user_df = self.replace_and_drop_null(user_df)
        user_df = self.drop_rows_containing_mask(user_df, "first_name", "\d+")
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'])
        user_df['email_address'] = user_df['email_address'].str.replace('@@', '@')
        user_df['country_code'] = user_df['country_code'].str.replace('GG', 'G')
        user_df['country_code'] = user_df['country_code'].astype('category')
        replacements = {'\(0\)': '', '[\)\(\.\- ]' : '', '^\+': '00'}
        user_df['phone_number'] = user_df['phone_number'].replace(replacements, regex=True)
        user_df = self.drop_rows_containing_mask(user_df, "phone_number", "[a-zA-Z]")
        user_df['join_date'] = pd.to_datetime(user_df['join_date'])
        user_df['phone_number'] = user_df['phone_number'].str.replace('^00\d{2}', '', regex=True)
        code_dict = {'GB': '0044', 'US': '001', 'DE': '0049'}
        user_df['phone_number'] = user_df['phone_number'].apply(lambda x: code_dict.get(user_df.loc[user_df['phone_number']==x, 'country_code'].values[0], '') + x)
        user_df = user_df.reset_index(drop=True)
        return user_df 
    
    def clean_store_data(self, store_df):
        store_df.drop('lat', axis=1, inplace=True)
        store_df = self.replace_and_drop_null(store_df)
        store_df = self.drop_rows_containing_mask(store_df, "staff_numbers", "[a-zA-Z]")
        store_df['continent'] = store_df['continent'].str.replace('ee', '')
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'])
        column_to_move = store_df.pop('latitude')
        store_df.insert(2, 'latitude', column_to_move)
        store_df['longitude'] = store_df['longitude'].astype('float')
        store_df['latitude'] = store_df['latitude'].astype('float')
        store_df['staff_numbers'] = store_df['staff_numbers'].astype('int')
        store_df['store_type'] = store_df['store_type'].astype('category')
        store_df['country_code'] = store_df['country_code'].astype('category')
        store_df = store_df.reset_index(drop=True)
        
        return store_df
    
    def convert_product_weights(weight):
        weight = str(weight)

        # Regular expressions to match different units and remove unwanted characters
        unit_conversions = {
            r'kg\b': 1,      # Kilograms
            r'x': '*',       # Conversion for 'x' used as a separator
            r'ml\b': 1/1000, # Milliliters to kilograms
            r'g\b': 1/1000,  # Grams to kilograms
            r'oz\b': 0.0283495  # Ounces to kilograms
        }

        for pattern, conversion in unit_conversions.items():
            if re.search(pattern, weight):
                weight = re.sub(rf'[\s,\'{pattern}]', "", weight)
                return float(eval(weight) if '*' in pattern else weight) * conversion

        # If no match is found, return the original weight as a string
        return weight
    
    def clean_products_data(self, product_df):
        product_df = self.replace_and_drop_null(product_df)
        product_df = self.drop_rows_containing_mask(product_df, "product_price", "[a-zA-Z]")
        product_df = product_df[product_df['EAN'].str.len() <= 13]
        product_df['date_added'] = pd.to_datetime(product_df['date_added'])
        product_df['weight'] = product_df['weight'].apply(self.convert_product_weights)
        product_df['weight'] = product_df['weight'].astype('float')
        product_df['product_price'] = product_df['product_price'].str.replace('£', '')
        product_df['product_price'] = product_df['product_price'].astype('float')
        product_df['category'] = product_df['category'].astype('category')
        product_df['removed'] = product_df['removed'].astype('category')
        product_df.rename(columns={'weight': 'weight_kg', 'product_price': 'price_£'}, inplace=True)
        product_df.drop('Unnamed: 0', axis=1, inplace=True)
        product_df = product_df.reset_index(drop=True)
        
        return product_df
    
    def clean_orders_data(self, order_df):
        order_df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        return order_df
    
    def clean_date_times_data(self, date_times_df):
        date_times_df = self.replace_and_drop_null(date_times_df)
        date_times_df = self.drop_rows_containing_mask(date_times_df, "month", "[a-zA-Z]")  
        
        return date_times_df           
