

def handler(event, context):
# check if there has been any updated parque file 
# in the processed bucket since last load


# for every unprocessed parque file convert to dataframe
# upload  
latest_update = get_previous_update_dt(table)
if latest_update:
    table_data = fetch_data_from_tables(conn, table, latest_update)
    if table_data:
        table_name, csv_data = convert_to_csv(table_data)
        written_table = write_to_s3(table_name, csv_data)
        updated_tables[table_name] = written_table
        logging.info(f"{table} has been updated. Pulling new data")
        update = True
else:
    table_data = fetch_data_from_tables(conn, table)
    table_name, csv_data = convert_to_csv(table_data)
    written_table = write_to_s3(table_name, csv_data, True)
    updated_tables[table_name] = written_table
    update = True
    logging.info(f"{table} has no initial data. Pulling data")
if not update:
logging.info("No need to update")
else:
lambda_client = boto3.client('lambda')
json_tables = json.dumps(updated_tables)
lambda_client.invoke(FunctionName='processing_handler', InvocationType='Event', Payload=json_tables)