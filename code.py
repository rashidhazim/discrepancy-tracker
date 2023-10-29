#v6.1.2
# Standard library imports
import re
from datetime import datetime

# Third-party imports
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from fuzzywuzzy import fuzz

CONN_PARAMS = {
        "user": "Hazim.Rashid@mckesson.com",
        "account": "mckesson.east-us-2.azure",
        "authenticator": "EXTERNALBROWSER",
        "role": "Sbx_ea_general_fr",
        "warehouse": "Sbx_ea_general_fr_wh",
        "database": "SBX_PSAS_DB",
        "schema": "ANALYTICS"
}

class SnowflakeOps:
    """
    A class to handle Snowflake operations like connecting, executing queries, and writing dataframes.
    """
    def __init__(self, params):
        self.conn_params = params
        self.conn = None
        self._cur = None

    def connect(self):
        """Establishes a connection to Snowflake."""
        self.conn = snowflake.connector.connect(**self.conn_params)
        self._cur = self.conn.cursor()

    def execute_query(self, query):
        """Executes a given SQL query and returns the result."""
        return self._cur.execute(query).fetchall()

    def write_dataframe(self, df, table_name):
        """Writes a dataframe to a given table in Snowflake."""
        write_pandas(self.conn, df, table_name)

    def close(self):
        """Closes the cursor and connection to Snowflake."""
        if self._cur:
            self._cur.close()
        if self.conn:
            self.conn.close()

def is_address_similar(addr1, addr2, threshold=90):
    """
    Check if two addresses are similar using fuzzy string matching.
    Args:
    - addr1: The first address string.
    - addr2: The second address string.
    - threshold: The similarity score threshold for considering the addresses as similar. Default is 90.
    
    Returns:
    - bool: True if the addresses are similar based on the threshold, else False.
    """
    similarity_score = fuzz.ratio(addr1, addr2)
    return similarity_score >= threshold

def normalize_address(address):
    address = re.sub(r'[^\w\s]', '', address).upper()
    
    # Step 1: Remove common suffixes in their short forms
    remove_short_suffixes = {
        r'\bST\b': '',
        r'\bAVE\b': '',
        r'\bRD\b': '',
        r'\bBLVD\b': '',
        r'\bDR\b': '',
        r'\bLN\b': '',
        r'\bCT\b': '',
        r'\bPL\b': '',
        r'\bTER\b': '',
        r'\bCIR\b': '',
        r'\bSQ\b': ''
    }
    
    for k, v in remove_short_suffixes.items():
        address = re.sub(k, v, address)
    
    # Step 2: Expand common abbreviations to their full forms
    expand_abbreviations = {
        r'\bST\b': 'STREET',
        r'\bAVE\b': 'AVENUE',
        r'\bRD\b': 'ROAD',
        r'\bBLVD\b': 'BOULEVARD',
        r'\bDR\b': 'DRIVE',
        r'\bLN\b': 'LANE',
        r'\bCT\b': 'COURT',
        r'\bPL\b': 'PLACE',
        r'\bTER\b': 'TERRACE',
        r'\bS\b': 'SOUTH',
        r'\bN\b': 'NORTH',
        r'\bE\b': 'EAST',
        r'\bW\b': 'WEST',
        r'\bAPT\b': 'APARTMENT',
        r'\bBLDG\b': 'BUILDING',
        r'\bDEPT\b': 'DEPARTMENT',
        r'\bPKWY\b': 'PARKWAY',
        r'\bEXPY\b': 'EXPRESSWAY',
        r'\bCIR\b': 'CIRCLE',
        r'\bHGTS\b': 'HEIGHTS',
        r'\bHL\b': 'HILL',
        r'\bJCT\b': 'JUNCTION',
        r'\bMT\b': 'MOUNT',
        r'\bSQ\b': 'SQUARE',
        r'\bFT\b': 'FORT',
        r'\bNW\b': 'NORTHWEST',
        r'\bNE\b': 'NORTHEAST',
        r'\bSW\b': 'SOUTHWEST',
        r'\bSE\b': 'SOUTHEAST'
    }
    
    for k, v in expand_abbreviations.items():
        address = re.sub(k, v, address)
    
    # Step 3: Remove common suffixes in their expanded forms
    remove_expanded_suffixes = {
        r'\bSTREET\b': '',
        r'\bAVENUE\b': '',
        r'\bROAD\b': '',
        r'\bBOULEVARD\b': '',
        r'\bDRIVE\b': '',
        r'\bLANE\b': '',
        r'\bCOURT\b': '',
        r'\bPLACE\b': '',
        r'\bTERRACE\b': '',
        r'\bCIRCLE\b': '',
        r'\bSQUARE\b': ''
    }
    
    for k, v in remove_expanded_suffixes.items():
        address = re.sub(k, v, address)
    
    suite_variations = [r'\bSTE\b', r'\bAPARTMENT\b', r'\bUNIT\b', r'#']
    for variant in suite_variations:
        address = re.sub(variant, ' SUITE ', address)
    
    # Removing any extra spaces
    address = ' '.join(address.split())
    
    return address

def categorize_discrepancy(row):
    discrepancies = []

    if row['Unique_Addresses_for_GLN'] > 1:
        discrepancies.append("Multiple Addresses for SHIP_TO_GLN")
    if row['SHIP_TO_GLN'] > 1:
        discrepancies.append("Multiple SHIP_TO_GLN per Address")

    return ', '.join(discrepancies)

def handle_discrepancies(df):
    df['Normalized_Address'] = df['ACCT_DLVRY_ADDR'].apply(normalize_address)

    # Detect GLNs with multiple addresses
    gln_grouped = df.groupby('SHIP_TO_GLN').agg({
        'Normalized_Address': pd.Series.nunique
    }).rename(columns={'Normalized_Address': 'Unique_Addresses_for_GLN'}).reset_index()

    df = df.merge(gln_grouped, on='SHIP_TO_GLN', how='left')

    # Create the Discrepancy column
    grouped = df.groupby('Normalized_Address').agg({
        'SHIP_TO_GLN': pd.Series.nunique,
        'Unique_Addresses_for_GLN': 'first'   # This ensures the columns are available
    }).reset_index()
    grouped['Discrepancy'] = grouped.apply(categorize_discrepancy, axis=1)
    
    df = df.merge(grouped[['Normalized_Address', 'Discrepancy']], on='Normalized_Address', how='left')

    # Identify similar addresses
    normalized_addresses = df['Normalized_Address'].tolist()
    similar_address_pairs = []

    for i, addr1 in enumerate(normalized_addresses):
        for j, addr2 in enumerate(normalized_addresses):
            if i != j and is_address_similar(addr1, addr2):
                similar_pair = (addr1, addr2)
                if similar_pair not in similar_address_pairs and (similar_pair[::-1] not in similar_address_pairs):
                    similar_address_pairs.append(similar_pair)

    # Adjusting the discrepancies based on similar addresses
    for addr1, addr2 in similar_address_pairs:
        addr1_discrepancy = df[df['Normalized_Address'] == addr1]['Discrepancy'].iloc[0]
        addr2_discrepancy = df[df['Normalized_Address'] == addr2]['Discrepancy'].iloc[0]
        
        if not addr2_discrepancy and addr1_discrepancy:
            df.loc[df['Normalized_Address'] == addr2, 'Discrepancy'] = addr1_discrepancy
        elif not addr1_discrepancy and addr2_discrepancy:
            df.loc[df['Normalized_Address'] == addr1, 'Discrepancy'] = addr2_discrepancy

    return df

def main():
    """
    Main function to execute the entire flow.
    """
    sf_ops = SnowflakeOps(CONN_PARAMS)
    sf_ops.connect()
    
    # Check if the CURRENT_DISCREPANCIES table exists
    check_table_query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'ANALYTICS'
        AND TABLE_NAME = 'CURRENT_DISCREPANCIES'
    """
    result = sf_ops.execute_query(check_table_query)[0][0]

    if result == 0:
        # If the table does not exist, create it
        create_table_query = """
            CREATE TABLE CURRENT_DISCREPANCIES (
                NATL_GRP_NAM TEXT,
                INA_LOC_ID TEXT,
                SHIP_TO_GLN TEXT,
                CUST_ACCT_ID TEXT,
                CUST_ACCT_NAM TEXT,
                ACCT_DLVRY_ADDR TEXT,
                ACCT_DLVRY_CTY_NAM TEXT,
                ACCT_DLVRY_ST_ABRV TEXT,
                ACCT_DLVRY_ZIP TEXT,
                DEA_NUM TEXT,
                CUST_CHN_ID TEXT,
                CUST_CHN_NAME TEXT,
                HOME_DC_ID TEXT,
                REP_NAME TEXT,
                VPS_NAME TEXT,
                Normalized_Address TEXT,
                Discrepancy TEXT,
                Date_Record_Pulled VARCHAR,
                NOTES TEXT,
                STATUS TEXT
            )
        """
        sf_ops.execute_query(create_table_query)

    check_table_query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'ANALYTICS' 
        AND TABLE_NAME = 'PREVIOUS_DISCREPANCIES'
    """
    result = sf_ops.execute_query(check_table_query)[0][0]

    if result == 0:
        create_table_query = """
            CREATE TABLE Previous_Discrepancies (
                CUST_ACCT_ID TEXT,
                Normalized_Address TEXT,
                INA_LOC_ID TEXT,
                SHIP_TO_GLN TEXT,
                Discrepancy TEXT
            )
        """
        sf_ops.execute_query(create_table_query)

    prev_discrepancy_query = "SELECT CUST_ACCT_ID FROM SBX_PSAS_DB.ANALYTICS.PREVIOUS_DISCREPANCIES"
    previous_discrepancies = pd.read_sql_query(prev_discrepancy_query, sf_ops.conn)

    query = """ 
-- Step 1: Create a temporary table with joined data
WITH joined_data AS (
    SELECT 
        a.NATL_GRP_NAM,
        a.SHIP_TO_GLN,
        a.CUST_ACCT_ID,
        a.CUST_ACCT_NAM,  
        a.DEA_NUM,        
        a.CUST_CHN_ID,    
        a.CUST_CHN_NAME,  
        a.HOME_DC_ID,     
        b.INA_LOC_ID,
        a.ACCT_DLVRY_ADDR,
        a.ACCT_DLVRY_CTY_NAM,
        a.ACCT_DLVRY_ST_ABRV,
        a.ACCT_DLVRY_ZIP,  -- added ACCT_DLVRY_ZIP
        c.REP_NAME,
        c.VPS_NAME
    FROM 
        SBX_PSAS_DB.SALES_OPS_GOV.T_CPH_MISSING_GLN_RPT a
    JOIN 
        PRD_PSAS_DB.RPT.T_LOCATION_MAP b
    ON 
        a.CUST_ACCT_ID = b.CUST_ACCT_ID
    LEFT JOIN
        SBX_PSAS_DB.ANALYTICS.T_CPH_ACCT_LEVEL_HIERARCHY_CURR c
    ON
        a.CUST_ACCT_ID = c.CUST_ACCT_ID
    WHERE 
        a.SHIP_TO_GLN IS NOT NULL
    
    UNION ALL
    
    SELECT 
        a.NATL_GRP_NAM,
        a.SHIP_TO_GLN,
        a.CUST_ACCT_ID,
        a.CUST_ACCT_NAM,  
        a.DEA_NUM,        
        a.CUST_CHN_ID,    
        a.CUST_CHN_NAME,  
        a.HOME_DC_ID,     
        b.INA_LOC_ID,
        a.ACCT_DLVRY_ADDR,
        a.ACCT_DLVRY_CTY_NAM,
        a.ACCT_DLVRY_ST_ABRV,
        a.ACCT_DLVRY_ZIP,  -- added ACCT_DLVRY_ZIP
        c.REP_NAME,
        c.VPS_NAME
    FROM 
        SBX_PSAS_DB.SALES_OPS_GOV.T_SA_MISSING_GLN_RPT a
    JOIN 
        PRD_PSAS_DB.RPT.T_LOCATION_MAP b
    ON 
        a.CUST_ACCT_ID = b.CUST_ACCT_ID
    LEFT JOIN
        SBX_PSAS_DB.ANALYTICS.T_CPH_ACCT_LEVEL_HIERARCHY_CURR c
    ON
        a.CUST_ACCT_ID = c.CUST_ACCT_ID
    WHERE 
        a.SHIP_TO_GLN IS NOT NULL
),

-- Step 2a: Group by NATL_GRP_NAM and INA_LOC_ID, and identify unique SHIP_TO_GLNs
grouped_data_ship_to_gln AS (
    SELECT 
        NATL_GRP_NAM,
        INA_LOC_ID,
        ARRAY_AGG(DISTINCT SHIP_TO_GLN) AS SHIP_TO_GLNs
    FROM 
        joined_data
    GROUP BY 
        NATL_GRP_NAM, INA_LOC_ID
    HAVING
        COUNT(DISTINCT SHIP_TO_GLN) > 1
),

-- Step 2b: Group by NATL_GRP_NAM and SHIP_TO_GLN, and identify unique INA_LOC_IDs
grouped_data_ina_loc_id AS (
    SELECT 
        NATL_GRP_NAM,
        SHIP_TO_GLN,
        ARRAY_AGG(DISTINCT INA_LOC_ID) AS INA_LOC_IDs
    FROM 
        joined_data
    GROUP BY 
        NATL_GRP_NAM, SHIP_TO_GLN
    HAVING
        COUNT(DISTINCT INA_LOC_ID) > 1
),

-- Step 3a: Normalize the arrays into separate rows for each SHIP_TO_GLN
normalized_data_ship_to_gln AS (
    SELECT 
        f.NATL_GRP_NAM,
        f.INA_LOC_ID,
        FLATTENED.value::STRING AS SHIP_TO_GLN
    FROM 
        grouped_data_ship_to_gln f,
        LATERAL FLATTEN(input => f.SHIP_TO_GLNs) AS FLATTENED
),

-- Step 3b: Normalize the arrays into separate rows for each INA_LOC_ID
normalized_data_ina_loc_id AS (
    SELECT 
        f.NATL_GRP_NAM,
        FLATTENED.value::STRING AS INA_LOC_ID,
        f.SHIP_TO_GLN
    FROM 
        grouped_data_ina_loc_id f,
        LATERAL FLATTEN(input => f.INA_LOC_IDs) AS FLATTENED
),

-- Union the results
union_normalized_data AS (
    SELECT * FROM normalized_data_ship_to_gln
    UNION ALL
    SELECT * FROM normalized_data_ina_loc_id
)

-- Step 4: Final output with additional details
SELECT 
    u.NATL_GRP_NAM,
    u.INA_LOC_ID,
    u.SHIP_TO_GLN,
    j.CUST_ACCT_ID,
    j.CUST_ACCT_NAM,  
    j.ACCT_DLVRY_ADDR,
    j.ACCT_DLVRY_CTY_NAM,
    j.ACCT_DLVRY_ST_ABRV,
    j.ACCT_DLVRY_ZIP,  -- added ACCT_DLVRY_ZIP
    j.DEA_NUM,        
    j.CUST_CHN_ID,    
    j.CUST_CHN_NAME,  
    j.HOME_DC_ID,     
    j.REP_NAME,
    j.VPS_NAME
  
FROM 
    union_normalized_data u
LEFT JOIN
    joined_data j
ON 
    u.NATL_GRP_NAM = j.NATL_GRP_NAM AND
    u.SHIP_TO_GLN = j.SHIP_TO_GLN AND
    u.INA_LOC_ID = j.INA_LOC_ID  
GROUP BY 
    u.NATL_GRP_NAM, u.INA_LOC_ID, u.SHIP_TO_GLN, j.CUST_ACCT_ID, j.CUST_ACCT_NAM,
    j.ACCT_DLVRY_ADDR, j.ACCT_DLVRY_CTY_NAM, j.ACCT_DLVRY_ST_ABRV,j.ACCT_DLVRY_ZIP,
    j.DEA_NUM, j.CUST_CHN_ID, j.CUST_CHN_NAME, j.HOME_DC_ID, j.REP_NAME, j.VPS_NAME

ORDER BY 
    u.NATL_GRP_NAM, u.INA_LOC_ID, u.SHIP_TO_GLN, j.CUST_ACCT_ID, j.CUST_ACCT_NAM,
    j.ACCT_DLVRY_ADDR, j.ACCT_DLVRY_CTY_NAM, j.ACCT_DLVRY_ST_ABRV,j.ACCT_DLVRY_ZIP,
    j.DEA_NUM, j.CUST_CHN_ID, j.CUST_CHN_NAME, j.HOME_DC_ID, j.REP_NAME, j.VPS_NAME;

"""
    data = sf_ops.execute_query(query)
    column_names = ["NATL_GRP_NAM", "INA_LOC_ID", "SHIP_TO_GLN", "CUST_ACCT_ID", "CUST_ACCT_NAM", "ACCT_DLVRY_ADDR", 
                    "ACCT_DLVRY_CTY_NAM", "ACCT_DLVRY_ST_ABRV", "ACCT_DLVRY_ZIP", "DEA_NUM", "CUST_CHN_ID", "CUST_CHN_NAME", "HOME_DC_ID",
                    "REP_NAME", "VPS_NAME"]
    df = pd.DataFrame(data, columns=column_names)

    df = handle_discrepancies(df)

    # Get the current date formatted as week-year
    current_date = datetime.now().strftime('%U-%Y')

    discrepancies_to_insert = df[['NATL_GRP_NAM', 'INA_LOC_ID', 'SHIP_TO_GLN', 'CUST_ACCT_ID', 'CUST_ACCT_NAM', 
                               'ACCT_DLVRY_ADDR', 'ACCT_DLVRY_CTY_NAM', 'ACCT_DLVRY_ST_ABRV', 'ACCT_DLVRY_ZIP', 'DEA_NUM', 'CUST_CHN_ID', 
                               'CUST_CHN_NAME', 'HOME_DC_ID', 'REP_NAME', 'VPS_NAME', 'Normalized_Address', 
                               'Discrepancy']]
    discrepancies_to_insert.columns = [col.upper() for col in discrepancies_to_insert.columns]

    # Add the current date to the discrepancies to insert
    discrepancies_to_insert['DATE_RECORD_PULLED'] = current_date

    # Insert new discrepancies into CURRENT_DISCREPANCIES table
    sf_ops.write_dataframe(discrepancies_to_insert, 'CURRENT_DISCREPANCIES')

    previous_account_ids = set(previous_discrepancies['CUST_ACCT_ID'])
    current_account_ids = set(df['CUST_ACCT_ID'])

    new_discrepancies = df[df['CUST_ACCT_ID'].isin(current_account_ids - previous_account_ids)]
    resolved_discrepancies = previous_discrepancies[previous_discrepancies['CUST_ACCT_ID'].isin(previous_account_ids - current_account_ids)]

    new_discrepancies.to_csv('new_discrepancies.csv', index=False)
    resolved_discrepancies.to_csv('resolved_discrepancies.csv', index=False)

    delete_query = "DELETE FROM Previous_Discrepancies"
    sf_ops.execute_query(delete_query)

    discrepancies_to_insert = df[['CUST_ACCT_ID', 'Normalized_Address', 'INA_LOC_ID', 'SHIP_TO_GLN', 'Discrepancy']]
    discrepancies_to_insert.columns = [col.upper() for col in discrepancies_to_insert.columns]
    sf_ops.write_dataframe(discrepancies_to_insert, 'PREVIOUS_DISCREPANCIES')

    df.drop(columns=['Unique_Addresses_for_GLN'], inplace=True)
    df.to_csv('analyzed_data.csv', index=False)

    sf_ops.close()

if __name__ == "__main__":
    main()