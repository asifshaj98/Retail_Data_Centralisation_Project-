import re
import pandas as pd
import numpy as np
class DataCleaning:
    def replace_and_drop_null(self, df):
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        
        return df
    
    def drop_rows_containing_mask(self, df, column, exp):
        mask = df[column].str.contains(exp)
        rows_to_drop = mask.index[mask].tolist()    
        df.drop(rows_to_drop, inplace=True)
        
        return df
    
    def clean_user_data(self, user_df):
        user_df = self.replace_and_drop_null(user_df)
        user_df = user_df.reset_index(drop=True)
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], errors='coerce')
        user_df.dropna(subset=['date_of_birth'], inplace=True)
        return user_df
    
    def clean_card_data(self, card_df):
        card_df = self.replace_and_drop_null(card_df)
        card_df.dropna(inplace=True)
        return card_df 
    
    def clean_store_data(self, store_df):
        store_df.drop('lat', axis=1, inplace=True)
        store_df = self.replace_and_drop_null(store_df)
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], errors='coerce')
        store_df.dropna(subset=['opening_date'], inplace=True)
        return store_df
    
    def convert_product_weights(self, weight):
        weight = str(weight)
        if re.search(r'kg\b', weight):
            x = re.sub("[\s,'kg']", "", weight)
            return float(x)
        elif re.search ('x', weight):
            weight = re.sub("x", "*", weight)
            y = re.sub("[\s,'g']", "", weight)
            z = eval(y)
            return float(z)/1000
        elif re.search (r'ml\b',weight):
            x = re.sub("[\s,'ml']", "", weight)
            return float(x)/1000
        elif re.search(r'g\b', weight):
            x = re.sub("[\s,'g']", "", weight)
            return float(x)/1000
        elif re.search(r'oz\b', weight):
            x = re.sub("[\s,'oz']", "", weight)
            return float(x)*0.0283495
        
        return weight
    
    def clean_products_data(self, product_df):
        product_df = self.replace_and_drop_null(product_df)
        product_df = product_df.reset_index(drop=True)
        return product_df
    
    def clean_orders_data(self, order_df):
        order_df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        return order_df           
    def clean_date_times_data(self, date_times_df):
            date_times_df = self.replace_and_drop_null(date_times_df)
            # Assuming you have a DataFrame called dim_date_times
            valid_time_periods = ['Late_Hours', 'Morning', 'Midday', 'Evening']
            date_times_df = date_times_df[date_times_df['time_period'].isin(valid_time_periods)]
            return date_times_df            