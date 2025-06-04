import oracledb
import pandas as pd
import os
import yaml
from dotenv import load_dotenv

# --- Load Environment Variables from .env file ---
load_dotenv()

# --- Azure Data Lake Upload Function (Provided by User) ---
# This function is defined here so the script knows about it if called later.
# Its actual implementation and required Azure libraries (azure.identity, azure.storage.filedatalake)
# would need to be present in the environment where this script runs.

# --- Function to Load YAML Configuration ---
def load_yaml_config(config_path="config.yaml"): # Changed from config.json
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) # Changed from json.load
        print(f"Configuration successfully loaded from '{config_path}'.")
        return config
    except FileNotFoundError:
        print(f"ERROR: Configuration file '{config_path}' not found. Please create it.")
        return None
    except yaml.YAMLError as e: # Changed from json.JSONDecodeError
        print(f"ERROR: Could not decode YAML from '{config_path}'. Check for syntax errors: {e}")
        return None
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while loading YAML configuration: {e}")
        return None

# --- Load Configurations ---
APP_CONFIG = load_yaml_config()
if not APP_CONFIG:
    print("Exiting due to missing or invalid YAML configuration.")
    exit()

# --- Database Credentials (from .env file) ---
DB_USER = os.getenv("ORACLE_DB_USER")
DB_PASSWORD = os.getenv("ORACLE_DB_PASSWORD")
DB_DSN = os.getenv("ORACLE_DB_DSN")

# --- Fabric Lakehouse Credentials (from .env file) ---
FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")

# --- Oracle Client Configuration for Thick Mode ---
ORACLE_CLIENT_LIB_DIR_ENV = os.getenv("ORACLE_CLIENT_LIB_DIR")
ORACLE_SETTINGS_JSON = APP_CONFIG.get("oracle_settings", {})
THICK_MODE_LIB_DIR_JSON = ORACLE_SETTINGS_JSON.get("thick_mode_lib_dir")
ORACLE_CLIENT_LIB_TO_USE = ORACLE_CLIENT_LIB_DIR_ENV if ORACLE_CLIENT_LIB_DIR_ENV else THICK_MODE_LIB_DIR_JSON

# --- Processing Settings (from config.json) ---
PROCESSING_SETTINGS = APP_CONFIG.get("processing_settings", {})
BASE_LOCAL_OUTPUT_DIR = PROCESSING_SETTINGS.get("base_local_output_directory", "parquet_output") # Renamed
TABLES_CONFIG_LIST = PROCESSING_SETTINGS.get("tables_to_process", []) # Now a list of objects
CAST_PRECISION = PROCESSING_SETTINGS.get("cast_number_to_precision", 22)
CAST_SCALE = PROCESSING_SETTINGS.get("cast_number_to_scale", 0)

# --- Fabric Lakehouse Settings (from config.json) ---
FABRIC_SETTINGS_JSON = APP_CONFIG.get("fabric_lakehouse_settings", {})
FABRIC_WORKSPACE_ID = FABRIC_SETTINGS_JSON.get("workspace_id")
FABRIC_LAKEHOUSE_ID = FABRIC_SETTINGS_JSON.get("lakehouse_id")


# --- Initialize Oracle Client for Thick Mode ---
try:
    if ORACLE_CLIENT_LIB_TO_USE:
        print(f"Attempting to initialize Oracle Client from: {ORACLE_CLIENT_LIB_TO_USE}")
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_TO_USE)
    else:
        print("Attempting to initialize Oracle Client from system path (no specific lib_dir provided).")
        oracledb.init_oracle_client()
    print("Successfully initialized Oracle Client in Thick Mode.")
except Exception as e:
    print(f"CRITICAL ERROR: Failed to initialize Oracle Client for Thick Mode: {e}")
    exit()

# --- Validate Essential Configurations ---
if not all([DB_USER, DB_PASSWORD, DB_DSN]):
    print("CRITICAL ERROR: Oracle credentials (ORACLE_DB_USER, ORACLE_DB_PASSWORD, ORACLE_DB_DSN) not found. Please set them in your .env file.")
    exit()

if not TABLES_CONFIG_LIST:
    print("WARNING: No tables specified in config.json under 'processing_settings.tables_to_process'.")

# Validate Fabric configs if uploads are intended
FABRIC_CONFIG_COMPLETE = all([FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_WORKSPACE_ID, FABRIC_LAKEHOUSE_ID])
if not FABRIC_CONFIG_COMPLETE:
    print("WARNING: Fabric Lakehouse connection details are incomplete (check .env for FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET and config.json for fabric_lakehouse_settings). Files will not be uploaded.")

def upload_file_to_datalake(tenant_id, client_id, client_secret, workspace_id, lakehouse_id, local_file_path, dest_folder_path, dest_file_name):
    """
    Uploads a file to Fabric Data Lake.

    Parameters:
    tenant_id (str): The Azure Active Directory tenant ID.
    client_id (str): The client ID of the Azure AD application.
    client_secret (str): The client secret of the Azure AD application.
    workspace_id (str): The id of the workspace in the Data Lake.
    lakehouse_id (str): The id of the lakehouse in the Data Lake.
    local_file_path (str): The local path of the file to be uploaded.
    dest_folder_path (str): The destination folder path within the lakehouse.
    dest_file_name (str): The name of the file in the destination folder.

    Returns:
    None
    """
    
    from azure.identity import ClientSecretCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    # Azure Data Lake Storage account details
    account_name = "onelake"
  
    # Construct the destination folder path within the lakehouse
    full_dest_folder_path = f"Files/{dest_folder_path}"

    # Construct the full path in the lakehouse
    data_path = f"{lakehouse_id}/{full_dest_folder_path}"

    # URL for the Data Lake Storage account
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"

    try:
        # Authenticate using ClientSecretCredential
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)

        # Create a DataLakeServiceClient instance
        service_client = DataLakeServiceClient(account_url, credential=credential)

        # Get a FileSystemClient for the specified workspace
        file_system_client = service_client.get_file_system_client(workspace_id)

        # Get a DirectoryClient for the specified path in the lakehouse
        directory_client = file_system_client.get_directory_client(data_path)

        # Create a FileClient for the file to be uploaded
        file_client = directory_client.create_file(dest_file_name)

        # Upload the local file to the Data Lake
        with open(local_file_path, "rb") as data:
            file_client.upload_data(data, overwrite=True)

        print(f"File '{dest_file_name}' uploaded successfully to '{data_path}'")

    except Exception as e:
        print(f"  ERROR uploading file '{local_file_path}' to Lakehouse: {e}")
        print(f"  Attempted Lakehouse path: {full_dest_folder_path}/{dest_file_name}")

def get_oracle_connection(user, password, dsn):
    """Establishes a connection to the Oracle database."""
    try:
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        print(f"Successfully connected to Oracle as user '{user}' with DSN '{dsn}'.")
        return conn
    except oracledb.DatabaseError as e:
        print(f"Error connecting to Oracle: {e}")
        return None

def get_table_column_metadata(connection, owner_name, table_name_only):
    """Retrieves column names and data types for a given table."""
    query = "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = :owner_name AND TABLE_NAME = :table_name_only ORDER BY COLUMN_ID"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, owner_name=owner_name.upper(), table_name_only=table_name_only.upper())
            return cursor.fetchall()
    except oracledb.DatabaseError as e:
        print(f"Error fetching metadata for {owner_name}.{table_name_only}: {e}")
        return []

def generate_select_query(owner_name, table_name_only, column_metadata, precision, scale):
    """Generates a SELECT query, casting NUMBER types."""
    if not column_metadata: return None
    select_clauses = [f'CAST("{c}" AS NUMBER({precision},{scale})) AS "{c}"' if dt == "NUMBER" else f'"{c}"' for c, dt in column_metadata]
    if not select_clauses: return None
    return f"SELECT\n  {',\n  '.join(select_clauses)}\nFROM \"{owner_name.upper()}\".\"{table_name_only.upper()}\""

def process_table(connection, oracle_table_full_name, local_output_dir, local_file_name_cfg, cast_prec, cast_scl):
    """
    Processes a single table: generates query, fetches data, saves to Parquet locally.
    Returns the full path to the locally saved Parquet file, or None on failure.
    """
    print(f"\nProcessing Oracle table: {oracle_table_full_name}...")

    if '.' in oracle_table_full_name:
        owner_name, table_name_only = oracle_table_full_name.split('.', 1)
    else:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
                fetched_owner = cursor.fetchone()
                if fetched_owner: owner_name = fetched_owner[0]
                else:
                    print(f"Could not determine current schema for table {oracle_table_full_name}. Skipping.")
                    return None
        except oracledb.DatabaseError as e:
            print(f"Error getting current schema for {oracle_table_full_name}: {e}. Skipping.")
            return None
        table_name_only = oracle_table_full_name
    
    owner_name_std = owner_name.upper()
    table_name_only_std = table_name_only.upper()

    column_metadata = get_table_column_metadata(connection, owner_name_std, table_name_only_std)
    if not column_metadata:
        print(f"Could not retrieve metadata for {owner_name_std}.{table_name_only_std}. Skipping.")
        return None
    print(f"  Column metadata retrieved for {oracle_table_full_name}.")

    select_query = generate_select_query(owner_name_std, table_name_only_std, column_metadata, cast_prec, cast_scl)
    if not select_query:
        print(f"Could not generate SELECT query for {owner_name_std}.{table_name_only_std}. Skipping.")
        return None

    try:
        print(f"  Executing query and fetching data for {oracle_table_full_name}...")
        df = pd.read_sql_query(select_query, connection)
        print(f"  Successfully fetched {len(df)} rows for {oracle_table_full_name}.")

        if not os.path.exists(local_output_dir):
            os.makedirs(local_output_dir)
            print(f"  Created local output directory: {local_output_dir}")

        # Construct full local file path using the configured local_file_name
        local_parquet_file_path = os.path.join(local_output_dir, local_file_name_cfg)

        df.to_parquet(local_parquet_file_path, index=False, engine='pyarrow')
        print(f"  Successfully saved data for {oracle_table_full_name} to local file: {local_parquet_file_path}")
        return local_parquet_file_path # Return the path for the upload step

    except oracledb.DatabaseError as e:
        print(f"  Oracle Error processing {oracle_table_full_name}: {e}")
    except pd.errors.DatabaseError as e: 
        print(f"  Pandas Database Error for {oracle_table_full_name}: {e}")
    except Exception as e:
        print(f"  An unexpected error occurred while processing {oracle_table_full_name}: {e}")
    return None

def main():
    """Main function to connect to Oracle, process tables, and upload to Fabric Lakehouse."""
    print("--- Starting Oracle to Parquet Script with Lakehouse Upload ---")
    
    conn = get_oracle_connection(DB_USER, DB_PASSWORD, DB_DSN)

    if conn:
        try:
            if not TABLES_CONFIG_LIST:
                print("No tables to process. Check 'tables_to_process' in config.json.")
            else:
                print(f"Base local output directory: {BASE_LOCAL_OUTPUT_DIR}")
                print(f"Casting NUMBER types to: NUMBER({CAST_PRECISION},{CAST_SCALE})")

                for table_config in TABLES_CONFIG_LIST:
                    oracle_table_name = table_config.get("oracle_table_name")
                    local_file_name = table_config.get("local_file_name")
                    lakehouse_folder = table_config.get("lakehouse_dest_folder_path")
                    lakehouse_file = table_config.get("lakehouse_dest_file_name")

                    if not all([oracle_table_name, local_file_name, lakehouse_folder, lakehouse_file]):
                        print(f"WARNING: Skipping table configuration due to missing fields: {table_config}")
                        continue
                    
                    # Process table and get local Parquet file path
                    local_parquet_path = process_table(
                        conn, 
                        oracle_table_name, 
                        BASE_LOCAL_OUTPUT_DIR, 
                        local_file_name, 
                        CAST_PRECISION, 
                        CAST_SCALE
                    )

                    if local_parquet_path and FABRIC_CONFIG_COMPLETE:
                        print(f"  Attempting to upload '{local_file_name}' to Lakehouse folder '{lakehouse_folder}' as '{lakehouse_file}'...")
                        upload_file_to_datalake(
                            tenant_id=FABRIC_TENANT_ID,
                            client_id=FABRIC_CLIENT_ID,
                            client_secret=FABRIC_CLIENT_SECRET,
                            workspace_id=FABRIC_WORKSPACE_ID,
                            lakehouse_id=FABRIC_LAKEHOUSE_ID,
                            local_file_path=local_parquet_path,
                            dest_folder_path=lakehouse_folder,
                            dest_file_name=lakehouse_file
                        )
                    elif local_parquet_path and not FABRIC_CONFIG_COMPLETE:
                        print(f"  Skipping upload for '{local_file_name}' due to incomplete Fabric configurations.")
                    else:
                        print(f"  Skipping upload for '{oracle_table_name}' as local Parquet file was not created.")
        finally:
            print("\nClosing Oracle connection.")
            conn.close()
    else:
        print("Could not establish Oracle connection. Exiting.")
    
    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()
