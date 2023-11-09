import pandas as pd
import datetime as datetime

def create_dim_date(start, end):
    given_date = datetime.datetime.strptime(start, "%Y/%m/%d") 
    
    df_date = pd.DataFrame({
        "date_id": [given_date.strftime("%Y%m%d")],
        "date": [given_date.strftime("%Y/%m/%d")],
        "year": [given_date.year],
        "month": [given_date.month],
        "day": [given_date.day],
        "day_of_week": [given_date.weekday()],
        "day_name": [given_date.strftime("%A")],
        "month_name": [given_date.strftime("%B")],
        "quarter": [(given_date.month-1)//3 + 1]
    })

    num_days = (datetime.datetime.strptime(end, "%Y/%m/%d") - datetime.datetime.strptime(start, "%Y/%m/%d")).days -1
    for i in range(num_days):
        given_date = given_date + datetime.timedelta(days=1)
        df_date.loc[len(df_date)] = {
            "date_id": given_date.strftime("%Y%m%d"),
            "date": given_date.strftime("%Y/%m/%d"),
            "year": given_date.year,
            "month": given_date.month,
            "day": given_date.day,
            "day_of_week": given_date.weekday(),
            "day_name": given_date.strftime("%A"),
            "month_name": given_date.strftime("%B"),
            "quarter": (given_date.month-1)//3 + 1
        }
    return df_date