import streamlit as st
import pandas as pd
import snowflake.connector

st.title('Discrepancy Tracker')

# Check if SnowflakeOps instance exists in session state
if 'sf_ops' not in st.session_state:
    # Define SnowflakeOps class
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

        def close(self):
            """Closes the cursor and connection to Snowflake."""
            if self._cur:
                self._cur.close()
            if self.conn:
                self.conn.close()

    # Define connection parameters
    CONN_PARAMS = {
        "user": "Hazim.Rashid@mckesson.com",
        "account": "mckesson.east-us-2.azure",
        "authenticator": "EXTERNALBROWSER",
        "role": "Sbx_ea_general_fr",
        "warehouse": "Sbx_ea_general_fr_wh",
        "database": "SBX_PSAS_DB",
        "schema": "ANALYTICS"
    }
    
    # Create SnowflakeOps instance and connect to Snowflake
    sf_ops = SnowflakeOps(CONN_PARAMS)
    sf_ops.connect()
    
    # Store SnowflakeOps instance in session state
    st.session_state['sf_ops'] = sf_ops
else:
    # Retrieve SnowflakeOps instance from session state
    sf_ops = st.session_state['sf_ops']

# Create a function to load data from Snowflake
def load_data():
    query = "SELECT * FROM CURRENT_DISCREPANCIES"
    data = sf_ops.execute_query(query)
    return data

# Create a function to update notes and status in Snowflake
def update_data(cust_acct_id, notes, status):
    query = f"""
    UPDATE CURRENT_DISCREPANCIES
    SET NOTES = '{notes}', STATUS = '{status}'
    WHERE CUST_ACCT_ID = '{cust_acct_id}'
    """
    sf_ops.execute_query(query)

# Initialize session state for data
if 'data' not in st.session_state:
    st.session_state['data'] = load_data()

# Load data
data = st.session_state['data']
column_names = ["NATL_GRP_NAM", "INA_LOC_ID", "SHIP_TO_GLN", "CUST_ACCT_ID", "CUST_ACCT_NAM",
                "ACCT_DLVRY_ADDR", "ACCT_DLVRY_CTY_NAM", "ACCT_DLVRY_ST_ABRV", "ACCT_DLVRY_ZIP",
                "DEA_NUM", "CUST_CHN_ID", "CUST_CHN_NAME", "HOME_DC_ID", "REP_NAME", "VPS_NAME",
                "NORMALIZED_ADDRESS", "DISCREPANCY", "DATE_RECORD_PULLED", "NOTES", "STATUS"]
df = pd.DataFrame(data, columns=column_names)

# Search
search_query = st.text_input("Search discrepancies", "")
if search_query:
    df_searched = df[df.apply(lambda row: search_query.lower() in row.astype(str).str.lower().to_string(), axis=1)]
else:
    df_searched = df

# Filter
filter_column = st.selectbox("Filter column", ["All"] + list(df.columns))
if filter_column == "All":
    df_filtered = df_searched
else:
    unique_values = df[filter_column].unique()
    selected_value = st.selectbox(f"Filter by {filter_column}", ["All"] + list(unique_values))
    if selected_value == "All":
        df_filtered = df_searched
    else:
        df_filtered = df_searched[df_searched[filter_column] == selected_value]

# UI elements for updating notes and status
cust_acct_id = st.selectbox("Select Customer Account ID", df['CUST_ACCT_ID'].unique())
notes = st.text_area("Add/Update Notes")

# Define common statuses
statuses = ["Open", "In Progress", "Resolved", "Closed"]
status = st.selectbox("Update Status", statuses)

# Update button
if st.button("Update"):
    update_data(cust_acct_id, notes, status)
    st.success("Data updated successfully!")
    
    # Reload data and update session state
    st.session_state['data'] = load_data()
    
    # Refresh dataframe and display updated data
    data = st.session_state['data']
    df = pd.DataFrame(data, columns=column_names)
    df_filtered = df[df['CUST_ACCT_ID'] == cust_acct_id]
    st.dataframe(df_filtered)
else:
    # Display the data in a table
    st.dataframe(df_filtered)
