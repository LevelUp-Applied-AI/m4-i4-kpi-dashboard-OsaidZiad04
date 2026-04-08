"""
Challenge Tier 2: Automated KPI Monitoring Script
Computes KPIs, compares to thresholds, outputs status, and generates Gauge visualizations.
"""
import os
import json
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

def load_config(filepath="config.json"):
    """Load baseline thresholds from config file."""
    with open(filepath, 'r') as f:
        return json.load(f)

def get_current_metrics():
    """Extract latest data to compute current KPI values."""
    db_url = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(db_url)
    
    orders = pd.read_sql_query("SELECT * FROM orders WHERE status != 'cancelled'", con=engine)
    order_items = pd.read_sql_query("SELECT * FROM order_items WHERE quantity <= 100", con=engine)
    customers = pd.read_sql_query("SELECT * FROM customers", con=engine)
    
    # Merge for revenue
    df = order_items.merge(orders, on='order_id')
    df['revenue'] = df['quantity'] * df['unit_price'] if 'unit_price' in df.columns else df['quantity'] * 50 # Fallback if price not in items
    
    # Current Metrics (Averages or latest for monitoring)
    current_metrics = {
        "monthly_revenue": df.groupby(pd.to_datetime(df['order_date']).dt.to_period('M'))['revenue'].sum().mean(),
        "weekly_orders": df.groupby(pd.to_datetime(df['order_date']).dt.to_period('W'))['order_id'].nunique().mean(),
        "aov": df.groupby('order_id')['revenue'].sum().mean(),
        "monthly_registrations": customers.groupby(pd.to_datetime(customers['registration_date']).dt.to_period('M'))['customer_id'].nunique().mean()
    }
    return current_metrics

def evaluate_status(current, target, warning_ratio):
    """Determine status color based on threshold."""
    if current >= target:
        return "green"
    elif current >= (target * warning_ratio):
        return "yellow"
    else:
        return "red"

def create_gauge_dashboard(metrics, config):
    """Create a Plotly figure with Gauge indicators and Dropdowns."""
    targets = config['targets']
    
    fig = go.Figure()
    
    # Create a gauge for each KPI
    for i, (kpi, current_val) in enumerate(metrics.items()):
        target_val = targets.get(kpi, current_val)
        status = evaluate_status(current_val, target_val, config['thresholds']['warning_ratio'])
        
        # Determine color
        color = "green" if status == "green" else "orange" if status == "yellow" else "red"
        
        fig.add_trace(go.Indicator(
            mode = "gauge+number+delta",
            value = current_val,
            title = {'text': kpi.replace('_', ' ').title()},
            delta = {'reference': target_val},
            gauge = {
                'axis': {'range': [None, target_val * 1.5]},
                'bar': {'color': color},
                'steps': [
                    {'range': [0, target_val * config['thresholds']['warning_ratio']], 'color': "lightgray"},
                    {'range': [target_val * config['thresholds']['warning_ratio'], target_val], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': target_val
                }
            },
            visible=(i==0) # Only first one visible initially
        ))

    # Add Dropdown Menu (updatemenus)
    buttons = []
    for i, kpi in enumerate(metrics.keys()):
        visibility = [False] * len(metrics)
        visibility[i] = True
        buttons.append(
            dict(
                label=kpi.replace('_', ' ').title(),
                method="update",
                args=[{"visible": visibility},
                      {"title": f"{kpi.replace('_', ' ').title()} Monitor"}]
            )
        )

    fig.update_layout(
        updatemenus=[dict(active=0, buttons=buttons, x=0.1, y=1.15)],
        title="Automated KPI Monitor (Select KPI from Dropdown)"
    )
    
    os.makedirs("output", exist_ok=True)
    fig.write_html("output/kpi_monitor_gauges.html")
    print("\n✅ Gauge Dashboard saved to output/kpi_monitor_gauges.html")

def main():
    print("Loading configuration...")
    config = load_config()
    
    print("Fetching current data from database...")
    try:
        metrics = get_current_metrics()
        
        print("\n--- Automated KPI Status Report ---")
        for kpi, val in metrics.items():
            target = config['targets'].get(kpi, val)
            status = evaluate_status(val, target, config['thresholds']['warning_ratio'])
            status_icon = "🟢" if status == "green" else "🟡" if status == "yellow" else "🔴"
            print(f"{status_icon} {kpi.replace('_', ' ').title()}: {val:.2f} (Target: {target})")
            
        create_gauge_dashboard(metrics, config)
        
    except Exception as e:
        print(f"Error connecting to DB or computing metrics: {e}")

if __name__ == "__main__":
    main()