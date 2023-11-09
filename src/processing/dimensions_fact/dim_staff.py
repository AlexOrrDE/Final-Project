def create_dim_staff(staff_merge_df):
    columns_to_keep = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address"
    ]

    dim_staff_df = staff_merge_df[columns_to_keep]

    return dim_staff_df
