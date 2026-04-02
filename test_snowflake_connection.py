import snowflake.connector
import os 
from dotenv import load_dotenv

load_dotenv()


def test_connection():
    try:
            conn = snowflake.connector.connect(
                account   = os.getenv("SNOWFLAKE_ACCOUNT"),
                user      = os.getenv("SNOWFLAKE_USER"),
                password  = os.getenv("SNOWFLAKE_PASSWORD"),
                database  = os.getenv("SNOWFLAKE_DATABASE"),
                schema    = os.getenv("SNOWFLAKE_SCHEMA"),
                warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
            )

            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()

            print(f"  Connected to Snowflake successfully!")
            print(f"  Version : {version[0]}")
            print(f"  Account : {os.getenv('SNOWFLAKE_ACCOUNT')}")
            print(f"  Database: {os.getenv('SNOWFLAKE_DATABASE')}")

            cursor.close()
            conn.close()

    except Exception as e:
        print(f"✗ Connection failed: {e}")

if __name__ == "__main__":
    test_connection()