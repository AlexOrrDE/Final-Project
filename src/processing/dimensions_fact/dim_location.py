def create_dim_location(address_df):
    """Transforms address table into dim_location."""

    column_name_mapping = {
        "address_id": "location_id",
    }

    address_df.rename(columns=column_name_mapping, inplace=True)

    columns_to_keep = [
        "location_id",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
    ]

    dim_location_df = address_df[columns_to_keep]

    return dim_location_df
