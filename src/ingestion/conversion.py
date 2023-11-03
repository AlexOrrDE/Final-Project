import pandas as pd


def convert_to_csv(table_data):
    """Converts data format to .csv.

    Typical usage example:

      file_name, csv_data = convert_to_csv(file_data)
    """
    table_name = table_data["table_name"]
    csv_data = pd.DataFrame(table_data["data"]).to_csv(index=False)
    return table_name, csv_data
