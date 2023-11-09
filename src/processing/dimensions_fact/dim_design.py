def create_dim_design(design_df):
    columns_to_keep = [
        "design_id",
        "design_name",
        "file_location",
        "file_name"]

    dim_design_df = design_df[columns_to_keep]

    return dim_design_df
