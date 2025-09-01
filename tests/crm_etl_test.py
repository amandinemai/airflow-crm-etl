import json
import pytest
from unittest.mock import MagicMock, patch
from etl import crm_etl

def test_extract_data():
    ti = MagicMock()
    # Call the Airflow task function
    crm_etl.extract_data(ti)
    
    # Assert XCom push was called with expected key
    ti.xcom_push.assert_called()
    args, kwargs = ti.xcom_push.call_args
    assert kwargs["key"] == "extracted_data"

def test_transform_data():
    ti = MagicMock()

    # Simulate data from previous task
    raw_data = '{"product_id":[1],"product_name":["Iphone"],"category":["Computers&Accessories|Accessories&Peripherals|Cables&Accessories|Cables|USBCables"]}'
    ti.xcom_pull.return_value = raw_data

    crm_etl.transform_data(ti)
    ti.xcom_push.assert_called()
    args, kwargs = ti.xcom_push.call_args
    assert kwargs["key"] == "transformed_data"

@patch("etl.crm_etl.psycopg2.connect")
def test_load_to_db(mock_connect):
    ti = MagicMock()
    
    # Simulate transformed data
    transformed_data = '{"category":["Computers&Accessories|Accessories&Peripherals|Cables&Accessories|Cables|USBCables"],"product_count":[150]}'
    ti.xcom_pull.return_value = transformed_data

     # Setup connection mock
    mock_conn = mock_connect.return_value.__enter__.return_value
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    
    # Add encoding attribute so execute_values works
    mock_cursor.connection.encoding = "UTF8"
    # Make mogrify return bytes instead of MagicMock
    mock_cursor.mogrify.return_value = b"(dummy)"

    # Run function
    crm_etl.load_to_db(ti)

    # Assert we did try to connect
    mock_connect.assert_called()