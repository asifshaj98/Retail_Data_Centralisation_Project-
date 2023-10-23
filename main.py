from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

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