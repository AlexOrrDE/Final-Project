import pandas as pd
import boto3
import io

def cp_transformer(df):

    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket='ingestion-data-bucket-marble')
    address_tables = [obj['Key'] for obj in response['Contents'] if "address/" in obj['Key']]

    address_data = s3.get_object(Bucket='ingestion-data-bucket-marble', Key=f'{address_tables[0]}')
    read_address_data = address_data['Body'].read().decode('utf-8')
    address_file = io.StringIO(read_address_data)
    add_df = pd.read_csv(address_file, index_col=False)

    # nested for loops maybe to match counterparty id with address 
    # for index, row1 in df.iterrows():
    #     for index, row2 in add_df.iterrows():
    #         if row1['legal_address_id'] == row2['address_id']:
    #             print(row1['counterparty_legal_name'], row2['address_line_1'])
                
        
    return_table = pd.merge(df, add_df, left_on='legal_address_id', right_on='address_id')

    return_table = return_table[["counterparty_id", "counterparty_legal_name", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]]

    return_table = return_table.rename(columns={
        "address_line_1": "counterparty_legal_address_line_1", 
        "address_line_2": "counterparty_legal_address_line_2", 
        "district": "counterparty_legal_district", 
        "city": "counterparty_legal_city", 
        "postal_code": "counterparty_legal_postal_code", 
        "country": "counterparty_legal_country", 
        "phone": "counterparty_legal_phone_number"
    })
    pd.set_option('display.max_columns', None)
    return return_table



s3 = boto3.client('s3')
counterparty_data = s3.get_object(Bucket='ingestion-data-bucket-marble', Key='2023/11/06/counterparty/14:55.csv')
read_counterparty_data = counterparty_data['Body'].read().decode('utf-8')
counterparty_file = io.StringIO(read_counterparty_data)

cp_df = pd.read_csv(counterparty_file, index_col=False)

print(cp_transformer(cp_df))

# counterparty_id, 
# counterparty_legal_name, 
# address_line_1 as counterparty_legal_address_line_1, address_line_2 as counterparty_legal_address_line_2, 
# district as counterparty_legal_district, 
# city as counterparty_legal_city, 
# postal_code as counterparty_legal_postal_code, 
# country as counterparty_legal_country, 
# phone as counterparty_legal_phone_number
# s3://ingestion-data-bucket-marble/2023/11/06/address/14:56.csv
# s3://ingestion-data-bucket-marble/2023/11/06/counterparty/14:55.csv