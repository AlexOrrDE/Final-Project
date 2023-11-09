from src.processing.dimensions_fact.dim_currency import create_dim_currency
import pandas as pd


def test_create_dim_currency():
    test_data = {
        "currency_id":[1,2],
        "currency_code":["USD", "GBP"],
        "created_at":["2023-11-09 00:00:00","2023-11-09 00:00:00"],
        "last_updated":["2023-11-08 00:00:00","2023-11-09 00:00:00"]
    }
    expected_result = {
        "currency_id": [1, 2],
        "currency_code": ["USD", "GBP"],
        "currency_name": ["US Dollar","British Pound"]
    }

    test_df = pd.DataFrame(test_data)

    output_df = create_dim_currency(test_df)


    pd.testing.assert_frame_equal(output_df, pd.DataFrame(expected_result))