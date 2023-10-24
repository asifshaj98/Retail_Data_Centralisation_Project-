from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import ssl

def main():
# Creating an instance of the DatabaseConnector, DataExtractor and DataCleaner classes.
    dbCON = DatabaseConnector()
    dbEX = DataExtractor()
    dbCLEAN = DataCleaning()
    

# Reading the legacy_users table from the database, cleaning the data and uploading it to the
# dim_users table.
    user_df = dbEX.read_rds_table(dbCON, 'legacy_users')
    clean_user_df = dbCLEAN.clean_user_data(user_df)
    dbCON.upload_to_db(clean_user_df, 'dim_users')
    
# This is getting the data from the api, cleaning the data and uploading it to the dim_store_details table.
    api_creds = dbCON.read_db_credentials('api_creds.yaml')
    store_df = dbEX.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', api_creds)
    clean_store_df = dbCLEAN.clean_store_data(store_df)
    dbCON.upload_to_db(clean_store_df, 'dim_store_details')  

# Reading the orders_table from the database, cleaning the data and uploading it to the orders_table.
    order_df = dbEX.read_rds_table(dbCON, 'orders_table')
    clean_order_df = dbCLEAN.clean_orders_data(order_df)
    dbCON.upload_to_db(clean_order_df, 'orders_table')

# This is reading the card_details.pdf file from the s3 bucket, cleaning the data and uploading it to
# the dim_card_details table.
    try:
        card_df = dbEX.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        clean_card_df = dbCLEAN.clean_card_data(card_df)
        dbCON.upload_to_db(clean_card_df, 'dim_card_details')
    except:
        print("error with card_df from pdf file")

# This is reading the products.csv file from the s3 bucket, cleaning the data and uploading it to
# the dim_products table.
    try:
        product_df = dbEX.extract_from_s3('s3://data-handling-public/products.csv')
        clean_product_df = dbCLEAN.clean_products_data(product_df)
        dbCON.upload_to_db(clean_product_df, 'dim_products')
    except:
        print("Error with product_df from S3 bucket")

# This is reading the date_details.json file from the s3 bucket, cleaning the data and uploading it to
# the dim_date_times table.
    try:
        json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        date_times_df = dbEX.extract_json_data(json_link)
        clean_date_times_df = dbCLEAN.clean_date_times_data(date_times_df)
        dbCON.upload_to_db(clean_date_times_df, 'dim_date_times')
    except:
        print("Error with extracting from JSON")
if __name__ == "__main__":
    main()

