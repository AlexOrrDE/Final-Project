import pandas as pd
import boto3
import io

def cp_transformer(df):
    s3 = boto3.client('s3')

    address_data = s3.get_object(Bucket='totesys-test', Key='2023/11/06/address/14:56.csv')
    read_address_data = address_data['Body'].read().decode('utf-8')

    address_file = io.StringIO(read_address_data)

    add_df = pd.read_csv(address_file, index_col=False)

    # print(cp_df, "COUNTER PARTY DF")
    # print(add_df, "ADDRESS DF")

    join = df.set_index('legal_address_id').join(add_df.set_index('address_id'), lsuffix='_cp', rsuffix='_add')

    join = join[["counterparty_id", "counterparty_legal_name", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]]

    join = join.rename(columns={
        "address_line_1": "counterparty_legal_address_line_1", 
        "address_line_2": "counterparty_legal_address_line_2", 
        "district": "counterparty_legal_district", 
        "city": "counterparty_legal_city", 
        "postal_code": "counterparty_legal_postal_code", 
        "country": "counterparty_legal_country", 
        "phone": "counterparty_legal_phone_number"
    })

    return join



s3 = boto3.client('s3')
counterparty_data = s3.get_object(Bucket='totesys-test', Key='2023/11/06/counterparty/14:55.csv')
read_counterparty_data = counterparty_data['Body'].read().decode('utf-8')
counterparty_file = io.StringIO(read_counterparty_data)

cp_df = pd.read_csv(counterparty_file, index_col=False)

print(cp_transformer(cp_df))