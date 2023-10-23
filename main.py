from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
# Creating an instance of the DatabaseConnector, DataExtractor and DataCleaner classes.
    dbcon = DatabaseConnector()
    dbex = DataExtractor()
    dbclean = DataCleaning()
    
# Reading the legacy_users table from the database, cleaning the data and uploading it to the
# dim_users table.
    user_df = dbex.read_rds_table(dbcon, 'legacy_users')
    clean_user_df = dbclean.clean_user_data(user_df)
    dbcon.upload_to_db(clean_user_df, 'dim_users')
    
# This is reading the card_details.pdf file from the s3 bucket, cleaning the data and uploading it to
# the dim_card_details table.
    card_df = dbex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    clean_card_df = dbclean.clean_card_data(card_df)
    dbcon.upload_to_db(clean_card_df, 'dim_card_details')
    
    
main()