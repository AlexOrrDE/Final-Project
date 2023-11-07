"""

handler(event, context)

    retrieve key and bucket names from event

    extract table name from key ----- 

    get the update data -- old_data = s3.get_object(Bucket=bucket_name, Key=old_data_filename)
    read the updated data -- read_old_data = old_data['Body'].read().decode('utf-8')

    convert data to file-like-object -- old_file = io.StringIO(read_old_data)

    convert file-like object to panda dataframe -- df = pd.read_csv(old_file, index_col=f"{table_name}_id")

    if table name design:
        return convert_to_parquet(design_transform_data function)
    elif table name counterparty:
        return convert_to_paruquet(counterparty_transform_data function)


    
    )
    # example flow of a table which relies on 1 source table
    design_transform_data(dataframe):
        relevant tables = [ source design table ]
        pull design table from s3 bucket
        convert to dataframe in same way as above
        transform dataframe to fit dim_design schema ---
        return transformed dataframe

        
    # example flow of a table which relies on 2 source tables
    counterparty_transform_data(dataframe):
        relevant tables = [ source counterparty table, source address table ]
        pull relevant tables from s3 bucket
        convert to dataframe in same way as above
        transform dataframe to fit dim_counterparty schema ---
        return transformed dataframe

    convert_to_parquet_function(transformed_dataframe):
        takes transformed data from previous function
        converts to parquet
        return parquet data

    write to s3(parquet_data):
        writes to s3 process bucket
        


"""