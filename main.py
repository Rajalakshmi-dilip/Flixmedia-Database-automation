import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from data.env
load_dotenv("data.env")

# Load SQL queries from Excel
excel_path = "C:/Users/rajalakshmi.d/OneDrive - Optisol Business Solutions Private Limited/Automation/Database automation/input.xlsx"
sheet_name = 'sheet 1'
queries_df = pd.read_excel(excel_path, sheet_name=sheet_name)

query_column_name = 'Queries'

# Add a new column for results
queries_df["Test result"] = "Test result"

# Get Redshift credentials from environment variables
db_user = os.getenv('REDSHIFT_USER')
db_password = os.getenv('REDSHIFT_PASSWORD')
db_host = os.getenv('REDSHIFT_HOST')
db_port = os.getenv('REDSHIFT_PORT')
db_name = os.getenv('REDSHIFT_DB')

# Validate that all environment variables are loaded
required_env_vars = {
    "REDSHIFT_USER": db_user,
    "REDSHIFT_PASSWORD": db_password,
    "REDSHIFT_HOST": db_host,
    "REDSHIFT_PORT": db_port,
    "REDSHIFT_DB": db_name
}

missing = [var for var, val in required_env_vars.items() if not val]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

# Connect using psycopg2
try:
    redshift_conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    print("Connection to Redshift established successfully.")
except Exception as conn_err:
    print(f"Failed to connect to Redshift: {conn_err}")
    raise

# Execute queries and save results in "Test result" column
for i, row in queries_df.iterrows():
    query = row[query_column_name]
    if pd.notna(query):
        try:
            result_df = pd.read_sql_query(query, redshift_conn)
            if not result_df.empty:
                # Store the first cell value (like <td>)
                first_value = result_df.iat[0, 0]
                queries_df.at[i, "Test result"] = str(first_value)
            else:
                queries_df.at[i, "Test result"] = "No data"
        except Exception as e:
            queries_df.at[i, "Test result"] = f"Failed: {str(e)}"

# Save to new Excel file
output_excel_path = os.path.join("output", "output.xlsx")
os.makedirs("output", exist_ok=True)
queries_df.to_excel(output_excel_path, index=False)
print(f"Results saved to: {output_excel_path}")

# Close connection
redshift_conn.close()
print("Redshift connection closed.")
