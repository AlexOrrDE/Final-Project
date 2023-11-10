import pandas as pd
import logging
import io

def convert_to_parquet(df):
    """Converts a pandas dataframe to parquet format
    
    Typical usage example:
    
        convert_to_parquet(df)
    """
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        return buffer

        return df.to_parquet('data.parquet', engine='fastparquet')
    
    except KeyError as ke:
        logging.error("Error occured in convert_to_parquet")
        raise ke


data = {
        'staff_id': [1, 2, 3, 4],
        'first_name': ['Jeremie', 'John', 'Jane', 'Jim'],
        'last_name': ['Franey', 'Doe', 'Doe', 'Beam'],
        'department_id': [2, 3, 4, 5],
        'email_address': ['jeremie.franey@example.com', 'john.doe@example.com', 
                          'jane.doe@example.com', 'jim.beam@example.com'],
        'created_at': [pd.Timestamp('2022-11-03 14:20:51.563000'), 
                       pd.Timestamp('2022-11-04 14:20:51.563000'), 
                       pd.Timestamp('2022-11-05 14:20:51.563000'), 
                       pd.Timestamp('2022-11-06 14:20:51.563000')],
        'last_updated': [pd.Timestamp('2022-11-03 14:20:51.563000'), 
                         pd.Timestamp('2022-11-04 14:20:51.563000'), 
                         pd.Timestamp('2022-11-05 14:20:51.563000'), 
                         pd.Timestamp('2022-11-06 14:20:51.563000')]
    }

df = pd.DataFrame(data)
buffer = convert_to_parquet(df)
# buffer.seek(0)
df2 = pd.read_parquet(filename, engine='fastparquet')
print(df, df2)
