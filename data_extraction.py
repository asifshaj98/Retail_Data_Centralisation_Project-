import pandas as pd
import tabula
import requests
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
    pass