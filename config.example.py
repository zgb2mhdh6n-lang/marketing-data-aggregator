"""
Configuration template for Marketing Data Aggregator
Copy to config.py and fill in your credentials
"""

# Database Configuration
DB_USER = "your_database_user"
DB_PASSWORD = "your_secure_password"
DB_PORT = 5432
APPLICATION_NAME = "marketing_data_aggregator"

# Database Connections
DATABASES = {
    "transactions": {
        "host": "172.16.x.xxx",
        "database": "transactions_db"
    },
    "auth_logs": {
        "host": "172.16.x.xx",
        "database": "auth_logs_db"
    }
}

# Merchant Configuration
MERCHANT_ID = "12345678000190"
DATE_WINDOW_DAYS = 10

# Output Configuration
OUTPUT_PATH = "/mnt/shared/marketing/resultado_transacoes.xlsx"
