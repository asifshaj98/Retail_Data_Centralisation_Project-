import pandas as pd
import tabula
import requests
import boto3
import re
class DataExtractor:
    
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine, index_col='index')
        return df
    
    def retrieve_pdf_data(self, link):
        df = pd.concat(tabula.read_pdf(link, pages='all'), ignore_index=True)
        
        return df
    
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        
        response = requests.get(num_stores_endpoint, headers=header_dict)
        data = response.json()
        number_of_stores = data['number_stores']
        
        return number_of_stores
    
    def retrieve_stores_data(self, num_stores_endpoint, stores_endpoint, header_dict):
        number_of_stores = self.list_number_of_stores(num_stores_endpoint, header_dict)
        
        for store_number in range(number_of_stores):
            
            if store_number == 0:
                response = requests.get(f'{stores_endpoint}/{store_number}', headers=header_dict)
                data = response.json()
                columns = list(data.keys())
                df = pd.DataFrame(data, columns=columns, index=[0])
                df.set_index("index", inplace=True)
                dfs = []
                dfs.append(df)
            else:
                response = requests.get(f'{stores_endpoint}/{store_number}', headers=header_dict)
                data = response.json()                
                df = pd.DataFrame(data, index=[0])
                df.set_index("index", inplace=True)
                dfs.append(df)
        
        df = pd.concat(dfs, ignore_index=True)
        
        return df
    def extract_from_s3(self, address):
        client = boto3.client('s3')
        df = pd.read_csv(address)
        
        return df
    
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
    
    pass