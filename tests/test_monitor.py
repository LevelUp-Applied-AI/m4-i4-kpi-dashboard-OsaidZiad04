import json
import pytest
from unittest.mock import patch, mock_open
import pandas as pd
from kpi_monitor import load_config, evaluate_status, get_current_metrics

def test_evaluate_status():
    """Verify threshold logic outputs correct green/yellow/red status."""
    target = 100
    warning_ratio = 0.8
    
    assert evaluate_status(110, target, warning_ratio) == "green"
    assert evaluate_status(100, target, warning_ratio) == "green"
    assert evaluate_status(85, target, warning_ratio) == "yellow"
    assert evaluate_status(70, target, warning_ratio) == "red"

def test_load_config():
    """Verify config format and loading."""
    mock_json = '{"targets": {"aov": 100}, "thresholds": {"warning_ratio": 0.8}}'
    with patch("builtins.open", mock_open(read_data=mock_json)):
        config = load_config("dummy.json")
        assert "targets" in config
        assert config["targets"]["aov"] == 100

@patch("kpi_monitor.create_engine")
@patch("pandas.read_sql_query")
def test_mock_db_connection(mock_read_sql, mock_engine):
    """Mock the database connection and verify get_current_metrics handles data."""
    # Mock return values for the 3 SQL queries
    mock_orders = pd.DataFrame({"order_id": [1, 2], "status": ["completed", "completed"], "order_date": ["2024-01-01", "2024-01-02"]})
    mock_items = pd.DataFrame({"order_id": [1, 2], "quantity": [1, 2], "unit_price": [50, 60]})
    mock_customers = pd.DataFrame({"customer_id": [1], "registration_date": ["2024-01-01"]})
    
    mock_read_sql.side_effect = [mock_orders, mock_items, mock_customers]
    
    metrics = get_current_metrics()
    
    assert "aov" in metrics
    assert "monthly_revenue" in metrics
    assert mock_read_sql.call_count == 3