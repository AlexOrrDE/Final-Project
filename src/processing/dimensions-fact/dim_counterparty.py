def create_dim_counterparty(cp_address_merge_df):
    column_name_mapping = {
        "address_line_1": "counterparty_legal_address_line_1",
        "address_line_2": "counterparty_legal_address_line_2",
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone-number",
    }

    cp_address_merge_df.rename(columns=column_name_mapping, inplace=True)

    columns_to_keep = [
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone-number",
    ]

    dim_counterparty_df = cp_address_merge_df[columns_to_keep]

    return dim_counterparty_df
