import pandas as pd
import tabula
class DataExtractor:
    
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine, index_col='index')
        return df
    
    def retrieve_pdf_data(self, link):
        df = pd.concat(tabula.read_pdf(link, pages='all'), ignore_index=True)
        
        return df
    pass