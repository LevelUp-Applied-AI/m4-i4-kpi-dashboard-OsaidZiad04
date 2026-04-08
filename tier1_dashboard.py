"""
Challenge Tier 1: Interactive Dashboard with Plotly
Converts static KPI charts into a standalone interactive HTML dashboard.
"""
import os
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

def get_kpi_data():
    """Extract data and compute KPIs (reusing logic from analysis.py)."""
    db_url = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(db_url)
    
    # Extract
    customers = pd.read_sql_table('customers', con=engine)
    products = pd.read_sql_table('products', con=engine)
    orders = pd.read_sql_table('orders', con=engine)
    order_items = pd.read_sql_table('order_items', con=engine)
    
    # Clean
    orders = orders[orders['status'] != 'cancelled'].copy()
    order_items = order_items[order_items['quantity'] <= 100].copy()
    
    # Merge
    df = order_items.merge(orders, on='order_id')\
                    .merge(products, on='product_id')\
                    .merge(customers, on='customer_id')
    df['revenue'] = df['quantity'] * df['unit_price']
    df['order_date'] = pd.to_datetime(df['order_date'])
    customers['registration_date'] = pd.to_datetime(customers['registration_date'])
    
    kpis = {}
    
    # KPI 1
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)
    kpis['monthly_revenue'] = df.groupby('order_month')['revenue'].sum().reset_index()
    
    # KPI 2
    df['order_week'] = df['order_date'].dt.to_period('W').astype(str)
    kpis['weekly_orders'] = df.groupby('order_week')['order_id'].nunique().reset_index()
    
    # KPI 3
    order_totals = df.groupby(['order_id', 'city'])['revenue'].sum().reset_index()
    kpis['aov_data'] = order_totals # Using raw data for boxplot
    
    # KPI 4
    kpis['revenue_by_category'] = df.groupby('category')['revenue'].sum().reset_index().sort_values(by='revenue', ascending=False)
    
    # KPI 5
    customers['reg_month'] = customers['registration_date'].dt.to_period('M').astype(str)
    kpis['monthly_registrations'] = customers.groupby('reg_month')['customer_id'].nunique().reset_index()
    
    return kpis

def create_interactive_dashboard():
    kpis = get_kpi_data()
    os.makedirs("output", exist_ok=True)
    
    print("Generating Plotly figures...")
    
    # 1. Monthly Revenue Trend (Line Chart)
    fig1 = px.line(kpis['monthly_revenue'], x='order_month', y='revenue', 
                   title='Monthly Revenue Trend', markers=True, 
                   labels={'order_month': 'Month', 'revenue': 'Revenue (JOD)'})
    
    # 2. Weekly Order Volume (Bar Chart)
    fig2 = px.bar(kpis['weekly_orders'], x='order_week', y='order_id', 
                  title='Weekly Order Volume', 
                  labels={'order_week': 'Week', 'order_id': 'Orders'})
    
    # 3. AOV by City (Box Plot)
    fig3 = px.box(kpis['aov_data'], x='city', y='revenue', color='city',
                  title='Order Value Distribution by City',
                  labels={'city': 'City', 'revenue': 'Order Value (JOD)'})
    
    # 4. Revenue by Category (Bar Chart)
    fig4 = px.bar(kpis['revenue_by_category'], x='category', y='revenue', color='category',
                  title='Total Revenue by Product Category',
                  labels={'category': 'Category', 'revenue': 'Total Revenue (JOD)'})
    
    # 5. Monthly Registrations (Scatter/Line)
    fig5 = px.line(kpis['monthly_registrations'], x='reg_month', y='customer_id', 
                   title='Customer Acquisition Over Time', markers=True,
                   labels={'reg_month': 'Month', 'customer_id': 'New Registrations'})
    fig5.update_traces(line_color='green')

    print("Saving to interactive HTML dashboard...")
    
    # Combine all into one HTML file
    html_content = f"""
    <html>
    <head>
        <title>Amman Digital Market - Interactive Dashboard</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f9f9f9; }}
            h1 {{ text-align: center; color: #333; }}
            .chart-container {{ background: white; margin-bottom: 30px; padding: 20px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
        </style>
    </head>
    <body>
        <h1>Amman Digital Market - Interactive KPI Dashboard</h1>
        <div class="chart-container">{fig1.to_html(full_html=False, include_plotlyjs='cdn')}</div>
        <div class="chart-container">{fig2.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart-container">{fig3.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart-container">{fig4.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart-container">{fig5.to_html(full_html=False, include_plotlyjs=False)}</div>
    </body>
    </html>
    """
    
    with open("output/dashboard.html", "w", encoding="utf-8") as f:
        f.write(html_content)
        
    print("Success! Open 'output/dashboard.html' in your web browser to view the interactive dashboard.")

if __name__ == "__main__":
    create_interactive_dashboard()