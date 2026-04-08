"""Integration 4 — KPI Dashboard: Amman Digital Market Analytics

Extract data from PostgreSQL, compute KPIs, run statistical tests,
and create visualizations for the executive summary.

Usage:
    python analysis.py
"""
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sqlalchemy import create_engine

# Set colorblind-safe palette for all plots
sns.set_palette("colorblind")
plt.style.use("seaborn-v0_8-whitegrid")


def connect_db():
    """Create a SQLAlchemy engine connected to the amman_market database."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # Using psycopg2 default for SQLAlchemy if psycopg isn't explicitly configured
        db_url = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(db_url)
    return engine


def extract_data(engine):
    """Extract all required tables from the database into DataFrames."""
    print("Extracting data from database...")
    customers = pd.read_sql_table('customers', con=engine)
    products = pd.read_sql_table('products', con=engine)
    orders = pd.read_sql_table('orders', con=engine)
    order_items = pd.read_sql_table('order_items', con=engine)

    # Data Cleaning
    initial_orders = len(orders)
    initial_items = len(order_items)
    
    orders = orders[orders['status'] != 'cancelled'].copy()
    order_items = order_items[order_items['quantity'] <= 100].copy()

    print(f"Data Extracted & Cleaned:")
    print(f" - Orders: {initial_orders} -> {len(orders)} (filtered cancelled)")
    print(f" - Order Items: {initial_items} -> {len(order_items)} (filtered quantity > 100)")

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


def compute_kpis(data_dict):
    """Compute the 5 KPIs defined in kpi_framework.md."""
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # Merge data to create a comprehensive DataFrame
    df = order_items.merge(orders, on='order_id')\
                    .merge(products, on='product_id')\
                    .merge(customers, on='customer_id')
    
    # Calculate revenue per item
    df['revenue'] = df['quantity'] * df['unit_price']
    
    # Ensure dates are datetime objects
    df['order_date'] = pd.to_datetime(df['order_date'])
    customers['registration_date'] = pd.to_datetime(customers['registration_date'])

    kpis = {}

    # KPI 1: Monthly Revenue (Time-based)
    df['order_month'] = df['order_date'].dt.to_period('M')
    kpis['monthly_revenue'] = df.groupby('order_month')['revenue'].sum().reset_index()
    kpis['monthly_revenue']['order_month'] = kpis['monthly_revenue']['order_month'].astype(str)

    # KPI 2: Weekly Order Volume (Time-based)
    df['order_week'] = df['order_date'].dt.to_period('W')
    kpis['weekly_orders'] = df.groupby('order_week')['order_id'].nunique().reset_index()
    kpis['weekly_orders']['order_week'] = kpis['weekly_orders']['order_week'].astype(str)

    # KPI 3: Average Order Value by City (Cohort-based)
    order_totals = df.groupby(['order_id', 'city'])['revenue'].sum().reset_index()
    kpis['aov_by_city'] = order_totals.groupby('city')['revenue'].mean().reset_index()
    kpis['aov_by_city'] = kpis['aov_by_city'].sort_values(by='revenue', ascending=False)

    # KPI 4: Total Revenue by Product Category (Cohort-based)
    kpis['revenue_by_category'] = df.groupby('category')['revenue'].sum().reset_index()
    kpis['revenue_by_category'] = kpis['revenue_by_category'].sort_values(by='revenue', ascending=False)

    # KPI 5: Customer Registrations per Month (Time-based)
    customers['reg_month'] = customers['registration_date'].dt.to_period('M')
    kpis['monthly_registrations'] = customers.groupby('reg_month')['customer_id'].nunique().reset_index()
    kpis['monthly_registrations']['reg_month'] = kpis['monthly_registrations']['reg_month'].astype(str)

    # Save the merged df and order_totals for statistical testing
    kpis['_raw_df'] = df 
    kpis['_order_totals'] = order_totals

    return kpis


def run_statistical_tests(kpi_results):
    """Run hypothesis tests to validate patterns in the data."""
    results = {}
    order_totals = kpi_results['_order_totals']
    df = kpi_results['_raw_df']

    # Test 1: Independent T-Test (Amman vs Irbid Order Values)
    # H0: No difference in average order value between Amman and Irbid
    # H1: There is a significant difference
    amman_revenue = order_totals[order_totals['city'] == 'Amman']['revenue']
    irbid_revenue = order_totals[order_totals['city'] == 'Irbid']['revenue']
    
    if len(amman_revenue) > 0 and len(irbid_revenue) > 0:
        t_stat, p_val = stats.ttest_ind(amman_revenue, irbid_revenue, equal_var=False)
        results['t_test_city'] = {
            "test": "Welch's t-test (Amman vs Irbid AOV)",
            "statistic": t_stat,
            "p_value": p_val,
            "interpretation": "Significant difference in AOV" if p_val < 0.05 else "No significant difference in AOV"
        }

    # Test 2: ANOVA (Revenue by Category)
    # H0: Average item revenue is the same across all product categories
    # H1: At least one category differs
    categories = df['category'].unique()
    cat_data = [df[df['category'] == cat]['revenue'] for cat in categories]
    
    if len(cat_data) > 1:
        f_stat, p_val_anova = stats.f_oneway(*cat_data)
        results['anova_category'] = {
            "test": "One-way ANOVA (Revenue across Categories)",
            "statistic": f_stat,
            "p_value": p_val_anova,
            "interpretation": "Significant difference across categories" if p_val_anova < 0.05 else "No significant difference"
        }

    return results


def create_visualizations(kpi_results, stat_results):
    """Create publication-quality charts for all 5 KPIs."""
    print("Generating visualizations...")
    os.makedirs("output", exist_ok=True)

    # 1. Monthly Revenue Trend
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=kpi_results['monthly_revenue'], x='order_month', y='revenue', marker='o', linewidth=2)
    plt.title("Monthly Revenue Trend: Consistent Growth Observed", fontsize=14, weight='bold')
    plt.xlabel("Month")
    plt.ylabel("Total Revenue (JOD)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("output/kpi1_monthly_revenue.png", dpi=300)
    plt.close()

    # 2. Weekly Order Volume
    plt.figure(figsize=(12, 6))
    sns.barplot(data=kpi_results['weekly_orders'], x='order_week', y='order_id', color='cornflowerblue')
    plt.title("Weekly Order Volume Shows Seasonal Spikes", fontsize=14, weight='bold')
    plt.xlabel("Week")
    plt.ylabel("Number of Orders")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("output/kpi2_weekly_orders.png", dpi=300)
    plt.close()

    # 3. AOV by City (using _order_totals for Boxplot to show distribution)
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=kpi_results['_order_totals'], x='city', y='revenue')
    plt.title("Average Order Value Varies Significantly by City", fontsize=14, weight='bold')
    plt.xlabel("City")
    plt.ylabel("Order Value (JOD)")
    plt.tight_layout()
    plt.savefig("output/kpi3_aov_by_city.png", dpi=300)
    plt.close()

    # 4. Revenue by Category
    plt.figure(figsize=(10, 6))
    sns.barplot(data=kpi_results['revenue_by_category'], x='category', y='revenue')
    plt.title("Electronics Dominate Total Revenue", fontsize=14, weight='bold')
    plt.xlabel("Product Category")
    plt.ylabel("Total Revenue (JOD)")
    plt.tight_layout()
    plt.savefig("output/kpi4_revenue_by_category.png", dpi=300)
    plt.close()

    # 5. Monthly Registrations
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=kpi_results['monthly_registrations'], x='reg_month', y='customer_id', marker='s', color='green')
    plt.title("Customer Acquisition Over Time", fontsize=14, weight='bold')
    plt.xlabel("Month")
    plt.ylabel("New Registrations")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("output/kpi5_customer_growth.png", dpi=300)
    plt.close()

    print("Visualizations saved to 'output/' directory.")


def main():
    """Orchestrate the full analysis pipeline."""
    try:
        # 1. Connect
        engine = connect_db()
        
        # 2. Extract
        data = extract_data(engine)
        
        # 3. Compute KPIs
        kpis = compute_kpis(data)
        
        # 4. Run Tests
        stats_results = run_statistical_tests(kpis)
        print("\n--- Statistical Test Results ---")
        for key, res in stats_results.items():
            print(f"Test: {res['test']}")
            print(f"  Statistic: {res['statistic']:.4f}")
            print(f"  P-value: {res['p_value']:.4e}")
            print(f"  Interpretation: {res['interpretation']}\n")
            
        # 5. Visualizations
        create_visualizations(kpis, stats_results)
        
        print("Analysis completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()