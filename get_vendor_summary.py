import sqlite3
import pandas as pd
import logging
from sqlalchemy import create_engine
from ingestion_db import ingest_db   # ✅ correct import

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """This function will merge different tables to get the overall vendor summary."""
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
      SELECT VendorNumber,
             SUM(Freight) AS FreightCost
      FROM vendor_invoice
      GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
      SELECT 
          p.VendorNumber,
          p.VendorName,
          p.Brand,
          p.Description,
          p.PurchasePrice,
          pp.Price AS ActualPrice,
          pp.Volume,
          SUM(p.Quantity) AS TotalPurchaseQuantity,
          SUM(p.Dollars) AS TotalPurchaseDollars
      FROM purchases p
      JOIN purchase_prices pp
          ON p.Brand = pp.Brand   
      WHERE p.PurchasePrice > 0
      GROUP BY 
          p.VendorNumber,
          p.VendorName,
          p.Brand,
          p.PurchasePrice,
          pp.Volume
    ),

    SalesSummary AS (
      SELECT
          VendorNo,
          Brand,
          SUM(SalesDollars) AS TotalSalesDollars,
          SUM(SalesPrice) AS TotalSalesPrice,
          SUM(SalesQuantity) AS TotalSalesQuantity,
          SUM(ExciseTax) AS TotalExciseTax
      FROM sales
      GROUP BY VendorNo, Brand
    )

    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalSalesQuantity,
        ss.TotalExciseTax,
        fs.FreightCost 
    FROM PurchaseSummary ps
    JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
       AND ps.Brand = ss.Brand
    JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary


def clean_data(df):
    """This function will clean the data."""
     # changing datatype to float
    df['Volume'] = df['Volume'].astype('float64')

    # filling missing value with 0
    df.fillna(0, inplace=True)
    # removing space
    df['VendorName'] = df['VendorName'].str.strip()
     df['Description'] = df['Description'].str.strip()

    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df


if __name__ == '__main__':
    # use same engine everywhere
    engine = create_engine('sqlite:///inventory.db')
    # creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor summary table......')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning data......')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data......')
    ingest_db(clean_df, 'vendor_sales_summary', conn)   # ✅ match with import
    logging.info('Complete')
