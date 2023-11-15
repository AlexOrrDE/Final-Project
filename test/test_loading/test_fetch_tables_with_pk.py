from src.loading.fetch_tables_with_pk import fetch_tables_with_pk
from pytest import raises


class MockConn:
    class cursor:
        def execute(self, *args):
            return

        def fetchall(self):
            return (
                ["dim_counterparty", "counterparty_id"],
                ["dim_currency", "currency_id"],
                ["dim_date", "date_id"],
                ["dim_design", "design_id"],
                ["dim_location", "location_id"],
                ["dim_payment_type", "payment_type_id"],
                ["dim_staff", "staff_id"],
                ["dim_transaction", "transaction_id"],
                ["fact_payment", "payment_record_id"],
                ["fact_purchase_order", "purchase_record_id"],
                ["fact_sales_order", "sales_record_id"],
            )


def test_should_raise_error_if_database_connection_is_invalid():
    result = fetch_tables_with_pk(MockConn)
    assert result

    class FakeConn:
        pass

    with raises(AttributeError):
        fetch_tables_with_pk(FakeConn)


def test_should_return_list_of_dictionaries():
    result = fetch_tables_with_pk(MockConn)
    assert isinstance(result, list)
    for table in result:
        assert isinstance(table, dict)


def test_should_return_dictionaries_with_correct_keys():
    result = fetch_tables_with_pk(MockConn)

    for table in result:
        assert "table_name" in table
        assert "primary_key" in table


def test_should_assign_correct_result_to_each_key():
    result = fetch_tables_with_pk(MockConn)

    for table in result:
        assert "dim" or "fact" in table["table_name"]
        assert "id" in table["primary_key"]
