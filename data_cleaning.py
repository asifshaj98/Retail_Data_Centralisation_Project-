import pandas as pd
import numpy as np

class DataCleaning:
    def replace_and_drop_null(self, df):
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        return df
    
    def clean_user_data(self, user_df):
        user_df = self.replace_and_drop_null(user_df)
        user_df = self.drop_rows_containing_mask(user_df, "first_name", "\d+")
        return user_df
    
    def drop_rows_containing_mask(self, df, column, exp):
        mask = df[column].str.contains(exp)
        rows_to_drop = mask.index[mask].tolist()    
        df.drop(rows_to_drop, inplace=True)
        
        return df
    
    def clean_card_data(self,card_df):
        # Remove rows with at least one NULL or missing value
        card_df = card_df.dropna()
        # Convert the card_number column to integer
        card_df['card_number'] = card_df['card_number'].astype(str)
        valid_card_providers = ["VISA 16 digit", "JCB 15 digit", "Discover", "VISA 13 digit", "American Express", "Mastercard", "Maestro", "VISA 19 digit", "Diners Club / Carte Blanche", "JCB 16 digit"]
        card_df = card_df[card_df['card_provider'].isin(valid_card_providers)]
        card_df['card_provider'] = card_df['card_provider'].astype('category')        
        card_df = card_df.reset_index(drop=True)
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')
        # Remove rows with NaN values in the date_payment_confirmed column
        card_df.dropna(subset=['date_payment_confirmed'], inplace=True)
        card_df = card_df.reset_index(drop=True)
        return card_df