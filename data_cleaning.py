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

    pass