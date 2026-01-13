"""
Marketing Data Aggregator
Multi-database ETL pipeline for transaction and authentication data

Author: Diogo Alves Fragoso
Description: Aggregates data from multiple PostgreSQL databases, performs
             cross-database joins, deduplication, and generates Excel reports
             for marketing analysis.
"""

#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
from sqlalchemy.exc import TimeoutError
import pandas as pd
from sys import exit
import datetime


class DatabaseConfig:
    """Database connection configuration"""
    
    USER = "db_user"
    PASSWORD = "db_password"
    APPLICATION_NAME = "marketing_data_aggregator"
    PORT = 5432
    
    # Database connection definitions
    CONNECTIONS = [
        {
            "host": "172.16.x.xxx",
            "database": "transactions_db",
            "query": """
                SELECT
                    c.customer_cpf AS num_cpf,
                    c.card_number AS numcartao,
                    t.transaction_date AS data_transa,
                    t.transaction_time AS hora_transa,
                    t.merchant_id AS des_cnpj_reduzido,
                    t.virtual_card_number AS num_cartao_virtual,
                    t.amount AS vlr_total
                FROM
                    transactions t
                INNER JOIN customer_cards c
                    ON c.card_number = t.card_number
                WHERE
                    t.merchant_id = '12345678000190'
                    AND t.transaction_date >= current_date - 10
            """,
            "var_name": "df_transactions"
        },
        {
            "host": "172.16.5.75",
            "database": "auth_logs_db",
            "query": """
                SELECT
                    l.card_number AS num_cartao,
                    to_char(l.log_date, 'DD/MM/YYYY') AS data,
                    to_char(l.log_time::time, 'HH24:MI:SS') AS hora,
                    l.response_code AS cod_resposta,
                    l.description AS des_log,
                    l.raw_data AS des_linha_recebimento
                FROM
                    authentication.transaction_logs l
                WHERE
                    l.log_date >= current_date - 10
            """,
            "var_name": "df_auth_logs"
        }
    ]


def create_database_connection(user, password, host, port, database, app_name):
    """
    Create SQLAlchemy engine for database connection
    
    Args:
        user (str): Database username
        password (str): Database password
        host (str): Database host
        port (int): Database port
        database (str): Database name
        app_name (str): Application name for connection tracking
    
    Returns:
        Engine: SQLAlchemy engine object
    """
    connection_url = (
        f"postgresql://{user}:{password}@{host}:{port}/{database}"
        f"?application_name={app_name}"
    )
    
    return create_engine(
        url=connection_url,
        connect_args={"options": "-c timezone=-3"}
    )


def fetch_data_from_databases(connections_config):
    """
    Connect to multiple databases and fetch data
    
    Args:
        connections_config (list): List of connection configurations
    
    Returns:
        dict: Dictionary of DataFrames keyed by var_name
    """
    dataframes = {}
    
    for conn_config in connections_config:
        host = conn_config["host"]
        database = conn_config["database"]
        query = conn_config["query"]
        var_name = conn_config["var_name"]
        
        try:
            print(f"Connecting to {database}...")
            
            # Create engine
            engine = create_database_connection(
                DatabaseConfig.USER,
                DatabaseConfig.PASSWORD,
                host,
                DatabaseConfig.PORT,
                database,
                DatabaseConfig.APPLICATION_NAME
            )
            
            # Validate query is string
            if not isinstance(query, str):
                raise ValueError(
                    f"Query must be string, received {type(query)} for {var_name}"
                )
            
            # Execute query and load into DataFrame
            df_result = pd.read_sql(query, engine)
            df_result = df_result.reset_index(drop=True)
            
            # Store in dictionary
            dataframes[var_name] = df_result
            print(f"✓ Data loaded into {var_name}")
            
            # Close connection
            engine.dispose()
            
        except Exception as ex:
            print(f"Error connecting to {database}: {ex}")
            exit()
    
    return dataframes


def process_virtual_card_numbers(df_transactions):
    """
    Process virtual card numbers (extract last digits)
    
    Args:
        df_transactions (DataFrame): Transactions DataFrame
    
    Returns:
        DataFrame: Processed DataFrame
    """
    df_transactions['num_cartao_virtual'] = (
        df_transactions['num_cartao_virtual']
        .astype(str)
        .str.slice(6)  # Extract relevant portion
    )
    return df_transactions


def filter_by_merchant(df_logs, merchant_id='042227623000141'):
    """
    Filter authentication logs by merchant ID
    
    Args:
        df_logs (DataFrame): Authentication logs DataFrame
        merchant_id (str): Merchant identifier to filter
    
    Returns:
        DataFrame: Filtered DataFrame
    """
    df_filtered = df_logs[
        df_logs['des_linha_recebimento'].str.contains(merchant_id, na=False)
    ]
    return df_filtered


def merge_transactions_with_logs(df_transactions, df_logs):
    """
    Merge transactions with authentication logs using two strategies:
    1. Direct card number match
    2. Virtual card number match
    
    Args:
        df_transactions (DataFrame): Transactions DataFrame
        df_logs (DataFrame): Authentication logs DataFrame
    
    Returns:
        DataFrame: Merged and deduplicated DataFrame
    """
    # Rename for consistency
    df_transactions = df_transactions.rename(columns={'numcartao': 'num_cartao'})
    
    # Strategy 1: Match by physical card number
    df_by_card = pd.merge(
        df_transactions,
        df_logs,
        on='num_cartao',
        how='inner',
        suffixes=('', '_log')
    )
    
    # Strategy 2: Match by virtual card number
    df_by_virtual = pd.merge(
        df_transactions,
        df_logs,
        left_on='num_cartao_virtual',
        right_on='num_cartao',
        how='inner',
        suffixes=('', '_log_virtual')
    )
    
    # Combine both strategies
    df_combined = pd.concat([df_by_card, df_by_virtual], ignore_index=True)
    
    # Remove duplicates based on transaction identifiers
    unique_transaction_cols = [
        'num_cpf',
        'num_cartao',
        'num_cartao_virtual',
        'vlr_total',
        'data_transa',
        'hora_transa'
    ]
    
    df_combined = df_combined.drop_duplicates(subset=unique_transaction_cols)
    
    # Sort by transaction date and time
    df_combined = df_combined.sort_values(
        by=['data_transa', 'hora_transa'],
        ascending=True
    )
    
    # Clean up unnecessary columns from merge
    columns_to_drop = [col for col in df_combined.columns if '_log' in col]
    df_combined = df_combined.drop(columns=columns_to_drop, errors='ignore')
    
    return df_combined


def identify_divergences(df_logs, df_final):
    """
    Identify records that exist in logs but not in final dataset
    
    Args:
        df_logs (DataFrame): Authentication logs
        df_final (DataFrame): Final merged dataset
    
    Returns:
        DataFrame: Divergent records
    """
    # Find logs without matching transactions
    divergent_logs = df_logs[
        ~df_logs['num_cartao'].isin(df_final['num_cartao'])
    ]
    
    divergent_logs = divergent_logs.sort_values(by=['data', 'hora'], ascending=True)
    
    return divergent_logs


def export_to_excel(df_divergent, df_final, output_path):
    """
    Export DataFrames to Excel file with multiple sheets
    
    Args:
        df_divergent (DataFrame): Divergent records
        df_final (DataFrame): Final consolidated transactions
        output_path (str): Output file path
    """
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_divergent.to_excel(writer, sheet_name='Divergentes', index=False)
            df_final.to_excel(writer, sheet_name='Transacoes Consolidadas', index=False)
        
        print(f"✓ Excel file '{output_path}' generated successfully")
        
    except Exception as e:
        print(f"Error saving Excel file: {e}")
        exit()


def main():
    """Main execution function"""
    
    print("=" * 60)
    print("Marketing Data Aggregator")
    print("=" * 60)
    
    # Fetch data from all databases
    print("\n1. Fetching data from databases...")
    dataframes = fetch_data_from_databases(DatabaseConfig.CONNECTIONS)
    
    df_transactions = dataframes.get("df_transactions")
    df_auth_logs = dataframes.get("df_auth_logs")
    
    # Process virtual card numbers
    print("\n2. Processing virtual card numbers...")
    df_transactions = process_virtual_card_numbers(df_transactions)
    
    # Filter logs by merchant
    print("\n3. Filtering authentication logs...")
    merchant_id = '042227623000141'
    df_filtered_logs = filter_by_merchant(df_auth_logs, merchant_id)
    
    # Merge transactions with logs
    print("\n4. Merging transactions with authentication logs...")
    df_final = merge_transactions_with_logs(df_transactions, df_filtered_logs)
    
    # Identify divergences
    print("\n5. Identifying divergent records...")
    df_divergent = identify_divergences(df_filtered_logs, df_final)
    
    # Export to Excel
    print("\n6. Exporting to Excel...")
    output_path = '/mnt/shared/marketing/resultado_transacoes.xlsx'
    export_to_excel(df_divergent, df_final, output_path)
    
    print("\n" + "=" * 60)
    print(f"Processing complete!")
    print(f"Total transactions: {len(df_final)}")
    print(f"Divergent records: {len(df_divergent)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

---

### **4. requirements.txt:**
```
sqlalchemy>=1.4.0
pandas>=1.3.0
psycopg2-binary>=2.9.0
openpyxl>=3.0.0
```

---

### **5. .gitignore:**
```
# Credentials
config.py
*.env
.env

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
dist/
*.egg-info/

# Data files
*.xlsx
*.xls
*.csv
*.db
*.sqlite3

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# Logs
*.log
logs/
