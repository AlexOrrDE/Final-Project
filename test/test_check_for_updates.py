from src.ingestion.check_for_updates import check_for_updates
from unittest.mock import patch

@patch('conn')
def test_should_return_None_when_no_updates_are_found(mock_connect):
    print(mock_connect)
