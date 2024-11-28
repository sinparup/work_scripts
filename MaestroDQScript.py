#!/apollo/sbin/envroot "$ENVROOT/bin/python"
#!/usr/bin/env python
 
import os
import logging
import requests
import smtplib
import pandas as pd
import sys
import json
import pgdb
from pgdb import connect
import re
from email.message import EmailMessage
from com.amazon.datanet.service.requestcontext import RequestContext
from bdt_content_py_utils.datanetUtilFiles.datanetClient import get_datanet_client
from com.amazon.datanet.service.createjobprofileresponse import CreateJobProfileResponse
from pyodinhttp import odin_material_retrieve, odin_retrieve, odin_retrieve_pair
from bdt_content_py_utils.commonUtils.DBConnection import DBConnection
from coral.coralrpchandler import CoralRpcEncoder
 
logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logging.info("Start")
 
# Constants
COOKIE_FILE = os.path.expanduser("~") + '/.midway/cookie'
 
def fetch_odin_credentials(odin):
    try:
        material_set = odin_retrieve(odin, "Credential")
        secret_key = material_set.data
        # print(secret_key)
        access_key = odin_material_retrieve(odin, "Principal")
        # print(access_key)
        if access_key != "":
            return access_key, secret_key
    except Exception as e:
        print(e.message)
        print("Programme Terminated.")
        sys.exit(1)
 
def get_redshift_credentials(odin):
    logging.debug(f"Getting Redshift Credentials for {odin}")
    try:
        red_user_bytes, red_password_bytes = fetch_odin_credentials(odin)
        red_user = red_user_bytes.decode('utf-8')
        red_password = red_password_bytes.decode('utf-8')  # Assuming password is a string
        return red_user, red_password
    except Exception as e:
        logging.error(f"Error fetching Redshift credentials: {e}")
        raise
 
def get_rs_conn(dbname, port=8192, odin="com.amazon.dw_rs_reporting_admin.keys"):
    logging.debug(f"Attempting connection to Redshift cluster {dbname}")
    
    try:
        red_user, red_password = get_redshift_credentials(odin)
        logging.debug(f"Got Redshift credentials for {dbname}")
        
        # Temporarily increase timeout for testing
        conn = pgdb.connect(
            database=dbname,
            host='dw-rsm-017.cv63a8urwqge.us-east-1.redshift.amazonaws.com',
            port=port,
            user=red_user,
            password=red_password,
            connect_timeout=120  # Increase timeout to 120 seconds
        )
        logging.debug(f"Connected to Redshift cluster {dbname}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Redshift cluster {dbname}: {e}")
        raise
 
 
def create_pandas_table(sql_query, conn, database):
    try:
        logging.debug(f'Executing SQL query: {sql_query}')
        # Create a cursor object
        cursor = conn.cursor()
        # Execute the query
        cursor.execute(sql_query)
        # Fetch all rows from the executed query
        rows = cursor.fetchall()
        # Get column names from the cursor description
        columns = [desc[0] for desc in cursor.description]
        # Convert the result to a pandas DataFrame
        table = pd.DataFrame(rows, columns=columns)
        # Close the cursor
        cursor.close()
        logging.debug(f'Retrieved table from database: {database}')
        print(f'Table preview: ', table.head())
        return table
    except Exception as e:
        logging.error(f'Error executing SQL query: {str(e)}')
        return None
 
def send_html_email(subject, sent_from, sent_to, body):
    email = EmailMessage()
    email['Subject'] = subject
    email['From'] = sent_from
    email['To'] = sent_to
    email.set_content(body, subtype='html')
    try:
        with smtplib.SMTP('localhost') as s:
            s.send_message(email)
    except Exception as ex:
        logging.exception(ex)
 
def fetch_table_columns(schema_name, table_name, rs_conn, db_name):
    sql_fetch_table_columns = f"""
    SELECT * FROM information_schema.columns 
    WHERE upper(table_schema)='{schema_name.upper()}' AND 
    upper(table_name)=UPPER('{table_name.upper()}') ORDER BY ordinal_position """
 
    logging.debug(f"Fetching columns for table {schema_name}.{table_name}")
    print('Calling create_pandas_table funtion....')
    table_columns_df = create_pandas_table(sql_fetch_table_columns, rs_conn, db_name)
    print('table_columns_df - ', table_columns_df)
    if table_columns_df.empty:
        logging.debug(f"Table {schema_name}.{table_name} not found. Exiting")
        sys.exit(127)
    table_columns_df = table_columns_df.sort_values(by=["ordinal_position"]).drop_duplicates()
    print('table_columns_df ', table_columns_df)
    return table_columns_df
 
def fetch_difference_columns(prod_schema_name, prod_table_name, test_schema_name, test_table_name, rs_conn, db_name):
    sql_fetch_common_columns = f"""
    SELECT * FROM information_schema.columns 
    WHERE upper(table_schema)='{prod_schema_name.upper()}' AND 
    upper(table_name)=UPPER('{prod_table_name.upper()}') 
    AND column_name IN (SELECT column_name FROM information_schema.columns
    WHERE upper(table_schema)='{test_schema_name.upper()}' AND 
    upper(table_name)=UPPER('{test_table_name.upper()}') 
    ) 
    ORDER BY ordinal_position 
    """
    logging.debug(f"Fetching columns between tables {prod_schema_name}.{prod_table_name} and {test_schema_name}.{test_table_name}")
    print('Calling create_pandas_table funtion....')
    common_columns_df = create_pandas_table(sql_fetch_common_columns, rs_conn, db_name)
    common_columns_df = common_columns_df.sort_values(by=["ordinal_position"]).drop_duplicates()
 
    sql_fetch_prod_only_columns= f"""
    SELECT * FROM information_schema.columns 
    WHERE upper(table_schema)='{prod_schema_name.upper()}' AND 
    upper(table_name)=UPPER('{prod_table_name.upper()}') 
    AND column_name NOT IN (SELECT column_name FROM information_schema.columns
    WHERE upper(table_schema)='{test_schema_name.upper()}' AND 
    upper(table_name)=UPPER('{test_table_name.upper()}') 
    )    
    ORDER BY ordinal_position
    """
    logging.debug(f"Fetching prod only columns")
    prod_only_columns_df = create_pandas_table(sql_fetch_prod_only_columns, rs_conn, db_name)
    prod_only_columns_df = prod_only_columns_df.sort_values(by=["ordinal_position"]).drop_duplicates()
 
    sql_test_prod_only_columns= f"""
    SELECT * FROM information_schema.columns 
    WHERE upper(table_schema)='{test_schema_name.upper()}' AND 
    upper(table_name)=UPPER('{test_table_name.upper()}') 
    AND column_name NOT IN (SELECT column_name FROM information_schema.columns
    WHERE upper(table_schema)='{prod_schema_name.upper()}' AND 
    upper(table_name)=UPPER('{prod_table_name.upper()}') 
    )    
    ORDER BY ordinal_position
    """
    logging.debug(f"Fetching test only columns")
    test_only_columns_df = create_pandas_table(sql_test_prod_only_columns, rs_conn, db_name)
    test_only_columns_df = test_only_columns_df.sort_values(by=["ordinal_position"]).drop_duplicates()
 
    return (
        common_columns_df.to_dict(orient='records'),
        prod_only_columns_df.to_dict(orient='records'),
        test_only_columns_df.to_dict(orient='records')
    )
 
def compare_columns_for_tables(prod_schema_name, prod_table_name, test_schema_name, test_table_name, rs_conn, db_name):
    try:
        prod_columns = fetch_table_columns_as_dict(prod_schema_name, prod_table_name, rs_conn, db_name)
        test_columns = fetch_table_columns_as_dict(test_schema_name, test_table_name, rs_conn, db_name)
        common_columns, prod_only_columns, test_only_columns = fetch_difference_columns(prod_schema_name,
                                                                                    prod_table_name,
                                                                                    test_schema_name,
                                                                                    test_table_name,
                                                                                    rs_conn,
                                                                                    db_name)
 
        has_snapshot_day = 'snapshot_day' in [col['column_name'] for col in common_columns]
        print('has_snapshot_day - ', has_snapshot_day)
        has_region_id = 'region_id' in [col['column_name'] for col in common_columns]
        print('has_region_id - ', has_region_id)
        has_dw_last_updated = 'dw_last_updated' in [col['column_name'] for col in common_columns]
        print('has_dw_last_updated - ', has_dw_last_updated)
 
        return (prod_columns, test_columns,
            prod_only_columns, test_only_columns,
            common_columns, has_snapshot_day,
            has_region_id, has_dw_last_updated)
 
    except Exception as e:
        logging.error(f"Error comparing columns: {e}")
        raise
    
def fetch_table_columns_as_dict(schema_name, table_name ,rs_conn, db_name):
    return fetch_table_columns(schema_name, table_name ,rs_conn, db_name).to_dict(orient='records')
 
request_context_user = RequestContext()
request_context_user.login_name = ''
 
def create_ext_job_profile(client, description, sql, edx_file_template, login_name) -> CreateJobProfileResponse:
    return client.create_job_profile(
        job_profile={
            '__type': "com.amazon.datanet.model#TransformJobProfile",
            'version_attributes': {
                'updated_by': login_name
            },
            "output":
                {"__type": "com.amazon.datanet.model#FileTemplate",
                 "fileTemplate": edx_file_template},
            'type': "TRANSFORM",
            'description': description,
            'sql': sql
        } , request_context=request_context_user
    )
 
NA_ext_job = {
    'job_type': "com.amazon.datanet.model#ExtractJob",
    'dbUser': "amzn:cdo:datanet-dbuser:booker_test_readonly",
    'description': '[NA] Test extract job',
    'freeForm': 'NA',
    'generateNFiles': 16,
    'group': 'DW Test Jobs',
    'hostGroup': 'GenericHostGroupXS',
    'logicalDB': 'L-DW-RS-LOAD',
    'owner': 'sukulma',
    'parallelism': 1,
    'partitionTypeId': 'REGION',
    'partitionValue': 1,
    'priority': 'HIGHEST',
    'profileId': 9695272,
    'timezone': 'America/Los_Angeles',
    'type': "TRANSFORM"
 
}
 
EU_ext_job = {
    'job_type': "com.amazon.datanet.model#ExtractJob",
    'dbUser': "amzn:cdo:datanet-dbuser:booker_test_readonly",
    'description': '[EU] Test extract job',
    'freeForm': 'EU',
    'generateNFiles': 16,
    'group': 'DW Test Jobs',
    'hostGroup': 'GenericHostGroupXS',
    'logicalDB': 'L-DW-RS-LOAD',
    'owner': 'sukulma',
    'parallelism': 1,
    'partitionTypeId': 'REGION',
    'partitionValue': 2,
    'priority': 'HIGHEST',
    'profileId': 9695272,
    'timezone': 'Europe/Paris',
    'type': "TRANSFORM"
 
}
 
FE_ext_job = {
    'job_type': "com.amazon.datanet.model#ExtractJob",
    'dbUser': "amzn:cdo:datanet-dbuser:booker_test_readonly",
    'description': '[FE] Test extract job',
    'freeForm': 'FE',
    'generateNFiles': 16,
    'group': 'DW Test Jobs',
    'hostGroup': 'GenericHostGroupXS',
    'logicalDB': 'L-DW-RS-LOAD',
    'owner': 'sukulma',
    'parallelism': 1,
    'partitionTypeId': 'REGION',
    'partitionValue': 3,
    'priority': 'HIGHEST',
    'profileId': 9695272,
    'timezone': 'Asia/Tokyo',
    'type': "TRANSFORM"
}
 
IN_ext_job = {
    'job_type': "com.amazon.datanet.model#ExtractJob",
    'dbUser': "amzn:cdo:datanet-dbuser:booker_test_readonly",
    'description': '[IN] Test extract job',
    'freeForm': 'IN',
    'generateNFiles': 16,
    'group': 'DW Test Jobs',
    'hostGroup': 'GenericHostGroupXS',
    'logicalDB': 'L-DW-RS-LOAD',
    'owner': 'sukulma',
    'parallelism': 1,
    'partitionTypeId': 'REGION',
    'partitionValue': 4,
    'priority': 'HIGHEST',
    'profileId': 9695272,
    'timezone': 'Asia/Kolkata',
    'type': "TRANSFORM"
 
}
SA_ext_job =  {
    'job_type': "com.amazon.datanet.model#ExtractJob",
    'dbUser': "amzn:cdo:datanet-dbuser:booker_test_readonly",
    'description': '[SA] Test extract job',
    'freeForm': 'SA',
    'generateNFiles': 16,
    'group': 'DW Test Jobs',
    'hostGroup': 'GenericHostGroupXS',
    'logicalDB': 'L-DW-RS-LOAD',
    'owner': 'sukulma',
    'parallelism': 1,
    'partitionTypeId': 'REGION',
    'partitionValue': 5,
    'priority': 'HIGHEST',
    'profileId': 9695272,
    'timezone': 'America/Sao_Paulo',
    'type': "TRANSFORM"
}
 
def create_dq_sql(db_name, prod_table, test_table_name, join_columns, login_name):
 
    exclude_column = input("Do you want to exclude any columns from the DQ? {Y/N}: ")
    if exclude_column.upper() == 'Y':
        excluded_cols = input("Enter the excluded columns (separated by commas): ")
        excluded_cols = set([col.strip() for col in excluded_cols.split(",")])
    else:
        excluded_cols = set()
 
    def generate_select_list(columns, excluded_cols):
        select_list = ',\n'.join([col['column_name'] for col in columns if col['column_name'] not in excluded_cols])
        return select_list
 
    db_name = db_name.lower()
    login_name = login_name.lower()
    prod_table_schema = prod_table.split(".")[0]
    prod_table_name = prod_table.split(".")[1]
    test_table_schema = test_table_name.split(".")[0]
    test_table_name = test_table_name.split(".")[1]
    join_columns_list = join_columns.split(",")
 
    try:
        logging.debug('Connecting to Cluster.......')
        rs_conn = get_rs_conn(db_name, port=8192, odin="com.amazon.dw_rs_reporting_admin.keys")
        logging.debug('Calling compare_columns_for_tables function.....')
        (prod_columns, test_columns, prod_only_columns, test_only_columns, common_columns, has_snapshot_day,
         has_region_id, has_dw_last_updated) = compare_columns_for_tables(
            prod_table_schema, prod_table_name, test_table_schema, test_table_name, rs_conn, db_name
        )
 
    except Exception as e:
        logging.error(f"Error in creating DQ SQL: {e}")
        raise
 
    output_sql = ""
    output_sql += f"""
    /*+ ETLM {{
    depend:{{
        update:[
            {{
                name:"{prod_table_schema.upper()}.{prod_table_name.upper()}",
                age:{{days: 0}}
            }},
            {{
                name:"{test_table_schema.upper()}.{test_table_name.upper()}",
                age:{{days: 0}}
            }}
        ]
    }}
    }}
    */
"""
 
    # Generate SQL for counting total records in production
    output_sql += "------------------------------------------------------------\n"
    output_sql += f"SELECT 'Total count in {prod_table.upper()} excluding specified columns';\n"
    output_sql += f"SELECT 'PRODUCTION' AS ENV, COUNT(*) FROM (\n"
    output_sql += f"SELECT {generate_select_list(prod_columns, excluded_cols)}\n"
    output_sql += f"FROM {prod_table_schema.upper()}.{prod_table_name.upper()}\n"
    output_sql += f") AS production_counts;\n"
 
    # Generate SQL for counting total records in testing
    output_sql += "------------------------------------------------------------\n"
    output_sql += f"SELECT 'Total count in {test_table_name.upper()} excluding specified columns';\n"
    output_sql += f"SELECT 'TESTING' AS ENV, COUNT(*) FROM (\n"
    output_sql += f"SELECT {generate_select_list(test_columns, excluded_cols)}\n"
    output_sql += f"FROM {test_table_schema.upper()}.{test_table_name.upper()}\n"
    output_sql += f") AS testing_counts;\n"
 
    output_sql += f"------------------------------------------------------------\n"
    output_sql += f"SELECT 'Region wise total count comparison between {prod_table_schema.upper()}.{prod_table_name.upper()} and {test_table_schema.upper()}.{test_table_name.upper()}';\n"
    
    if has_region_id:
        output_sql += f"""
        SELECT region_id, 'PRODUCTION' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(prod_columns, excluded_cols)}, region_id
        FROM {prod_table_schema.upper()}.{prod_table_name.upper()}
        GROUP BY region_id
        )
        UNION ALL
        SELECT region_id, 'TESTING' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(test_columns, excluded_cols)}, region_id
        FROM {test_table_schema.upper()}.{test_table_name.upper()}
        GROUP BY region_id
        )
        ORDER BY 1, 2;\n
        """
    
        if has_snapshot_day:
            output_sql += f"""------------------------------------------------------------\n"""
            output_sql += f"""SELECT 'Count comparison between {prod_table_schema.upper()}.{prod_table_name.upper()} and {test_table_schema.upper()}.{test_table_name.upper()} for snapshot_day' || TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD');\n"""
            output_sql += f"""
        SELECT 'PRODUCTION' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(prod_columns, excluded_cols)}
        FROM {prod_table_schema.upper()}.{prod_table_name.upper()}
        WHERE SNAPSHOT_DAY = TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD')
        ) AS production_counts
        UNION ALL
        SELECT 'TESTING' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(test_columns, excluded_cols)}
        FROM {test_table_schema.upper()}.{test_table_name.upper()}
        WHERE SNAPSHOT_DAY = TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD')
        ) AS testing_counts
        ORDER BY 1;\n
        """
            if has_region_id:
                output_sql += f"""------------------------------------------------------------\n"""
                output_sql += f"""SELECT 'Region wise count comparison between {prod_table_schema.upper()}.{prod_table_name.upper()} and {test_table_schema.upper()}.{test_table_name.upper()} for snapshot_day' || TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD');\n"""
                output_sql += f"""
        SELECT region_id, 'PRODUCTION' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(prod_columns, excluded_cols)}
        FROM {prod_table_schema.upper()}.{prod_table_name.upper()}
        WHERE SNAPSHOT_DAY = TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD') group by 1,2
        ) AS production_counts
        UNION ALL
        SELECT region_id, 'TESTING' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(test_columns, excluded_cols)}
        FROM {test_table_schema.upper()}.{test_table_name.upper()}
        WHERE SNAPSHOT_DAY = TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD') group by 1,2
        ) AS testing_counts
        ORDER BY 1;\n
        """
                
        elif has_dw_last_updated:
            output_sql += f"""------------------------------------------------------------\n"""
            output_sql += f"""SELECT 'Count comparison between {prod_table_schema.upper()}.{prod_table_name.upper()} and {test_table_schema.upper()}.{test_table_name.upper()} for DW_LAST_UPDATED >= ' || to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS');\n"""
            output_sql += f"""
        SELECT 'PRODUCTION' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(prod_columns, excluded_cols)}
        FROM {prod_table_schema.upper()}.{prod_table_name.upper()}
        WHERE DW_LAST_UPDATED >= to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS')
        ) AS production_counts
        UNION ALL
        SELECT 'TESTING' AS ENV, COUNT(*) FROM (
        SELECT {generate_select_list(test_columns, excluded_cols)}
        FROM {test_table_schema.upper()}.{test_table_name.upper()}
        WHERE DW_LAST_UPDATED >= to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS')
        ) AS testing_counts
        ORDER BY 1;\n
        """
            if has_region_id:
                output_sql += f"""------------------------------------------------------------\n"""
                output_sql += f"""SELECT 'Region wise count comparison between {prod_table_schema.upper()}.{prod_table_name.upper()} and {test_table_schema.upper()}.{test_table_name.upper()} for DW_LAST_UPDATED >= ' || to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS');\n"""
                output_sql += f"""
            SELECT region_id, 'PRODUCTION' AS ENV, COUNT(*) FROM (
            SELECT {generate_select_list(prod_columns, excluded_cols)}
            FROM {prod_table_schema.upper()}.{prod_table_name.upper()}
            WHERE DW_LAST_UPDATED >= to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS')
            GROUP BY region_id
            ) AS prod_region_counts
            UNION ALL
            SELECT region_id, 'TESTING' AS ENV, COUNT(*) FROM (
            SELECT {generate_select_list(test_columns, excluded_cols)}
            FROM {test_table_schema.upper()}.{test_table_name.upper()}
            WHERE DW_LAST_UPDATED >= to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS')
            GROUP BY region_id
            ) AS test_region_counts
            ORDER BY 1, 2;\n
            """         
                    
        output_sql += f"""------------------------------------------------------------\n"""
        output_sql += f"""/*Columns present in  {prod_table_schema.upper()}.{prod_table_name.upper()} , but not in {test_table_schema.upper()}.{test_table_name.upper()}: {','.join([col['column_name'] for col in prod_only_columns])}*/\n"""
        output_sql += f"""/*Columns present in  {test_table_schema.upper()}.{test_table_name.upper()} , but not in {prod_table_schema.upper()}.{prod_table_name.upper()}: {','.join([col['column_name'] for col in test_only_columns])}*/\n"""
        output_sql += f"""/*Number of columns present in  {prod_table_schema.upper()}.{prod_table_name.upper()} : {len(prod_columns)}*/\n"""
        output_sql += f"""/*Number of columns present in  {test_table_schema.upper()}.{test_table_name.upper()} : {len(test_columns)}*/\n"""
        output_sql += f"""/*Number of columns present in both tables : {len(common_columns)}*/\n"""
        output_sql += f"""/*Only common columns in both tables are used for MD5 comparison.*/\n"""
        output_sql += f"""------------------------------------------------------------\n"""
        
        
        output_sql += f"""
        CREATE TEMPORARY TABLE PRD_MD5 DISTSTYLE KEY DISTKEY({join_columns_list[0].upper()}) AS 
        SELECT 
        """
        select_list= ',\n'.join([col['column_name'] for col in common_columns if col['column_name'] not in excluded_cols])
        #print(select_list)
     
        md5_list=[]
        for col in common_columns:
            column_name=col['column_name']
            data_type=col['data_type']
            if data_type in ['bigint', 'integer', 'int', 'smallint', 'decimal(38,0)']:
                column_name = f"TRIM(NVL({column_name},-1))"
            elif data_type in ['character varying','character']:
                column_name = f"TRIM(NVL({column_name},'-1'))"
            elif data_type in ['date']:
                column_name = f"TRIM(NVL({column_name},to_date('1970-01-01','YYYY-MM-DD')))"
            elif data_type in ['timestamp without time zone']:
                column_name = f"TRIM(NVL({column_name},TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')))"
            elif data_type in ['numeric']:
                column_name = f"TRIM(NVL({column_name},-1.0))"
            if col['column_name'] in ['dw_last_updated', 'snapshot_day', 'dw_creation_date']:
                column_name = '--' + column_name
            md5_list.append(column_name)
     
        output_sql += select_list
        output_sql += ',\n'
        output_sql += 'MD5('
        output_sql += '||\n'.join(md5_list)
        output_sql += '\n) AS md5_col \n'
     
        if has_snapshot_day:
            output_sql += f" FROM {prod_table_schema.upper()}.{prod_table_name.upper()} " \
                          f"WHERE SNAPSHOT_DAY = TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD');"
            output_sql += '\n'
        elif has_dw_last_updated:
            output_sql += f" FROM {prod_table_schema.upper()}.{prod_table_name.upper()} " \
                          f"WHERE DW_LAST_UPDATED >= to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS');"
            output_sql += '\n'
        else:
            output_sql += f" FROM {prod_table_schema.upper()}.{prod_table_name.upper()};"
            output_sql += '\n'
     
     
        output_sql += f"""
        CREATE TEMPORARY TABLE TST_MD5 DISTSTYLE KEY DISTKEY({join_columns_list[0].upper()}) AS 
        SELECT 
        """
     
        output_sql += select_list
        output_sql += ',\n'
        output_sql += 'MD5('
        output_sql += '||\n'.join(md5_list)
        output_sql += '\n) AS md5_col \n'
     
        if has_snapshot_day:
            output_sql += f" FROM {test_table_schema.upper()}.{test_table_name.upper()} " \
                          f"WHERE SNAPSHOT_DAY = TO_DATE('{{RUN_DATE_YYYY/MM/DD}}','YYYY/MM/DD');"
            output_sql += '\n'
        elif has_dw_last_updated:
            output_sql += f" FROM {test_table_schema.upper()}.{test_table_name.upper()} " \
                          f"WHERE DW_LAST_UPDATED >= to_timestamp('{{END_OF_DAY_AS_UTC}}','YYYY/MM/DD HH24:MI:SS');"
            output_sql += '\n'
        else:
            output_sql += f" FROM {test_table_schema.upper()}.{test_table_name.upper()}; "
            output_sql += '\n'
     
        output_sql += f"""\n------------------------------------------------------------\n"""
        output_sql += f"""CREATE TEMPORARY TABLE DQ_RESULTS DISTSTYLE KEY DISTKEY({join_columns_list[0].upper()})
        AS 
        SELECT 
        """
     
        output_sql += ',\n'.join([f"COALESCE(PRD_MD5.{col.upper()},TST_MD5.{col.upper()}) AS {col.upper()} " for col in join_columns_list])
        output_sql += """,
        CASE 
        WHEN PRD_MD5.md5_col IS NULL THEN 'MISSING FROM PROD'
        WHEN TST_MD5.md5_col IS NULL THEN 'MISSING FROM TEST'
        WHEN PRD_MD5.md5_col <> TST_MD5.md5_col THEN 'NOT MATCHING'
        WHEN PRD_MD5.md5_col = TST_MD5.md5_col THEN 'MATCHING'
        ELSE 'WHAT??'
        END AS DQ_FLAG
        FROM 
        PRD_MD5
        FULL OUTER JOIN 
        TST_MD5
        ON
        """
        output_sql += ' AND \n'.join(
            [f"PRD_MD5.{col.upper()} = TST_MD5.{col.upper()}" for col in join_columns_list])
        output_sql += "\n;\n"
        output_sql += f"""------------------------------------------------------------\n"""
        output_sql += f"""SELECT DQ_FLAG,COUNT(*) FROM DQ_RESULTS GROUP BY DQ_FLAG;\n"""
        output_sql += f"""------------------------------------------------------------\n"""
        output_sql += f"""SELECT * FROM (SELECT * FROM DQ_RESULTS WHERE DQ_FLAG='MISSING FROM TEST' LIMIT 10);\n"""
        output_sql += f"""------------------------------------------------------------\n"""
        output_sql += f"""SELECT * FROM (SELECT * FROM DQ_RESULTS WHERE DQ_FLAG='MISSING FROM PROD' LIMIT 10);\n"""
        output_sql += f"""------------------------------------------------------------\n"""
        output_sql += f"""SELECT * FROM (SELECT * FROM DQ_RESULTS WHERE DQ_FLAG='NOT MATCHING' LIMIT 10);\n"""
        output_sql += f"""------------------------------------------------------------\n"""
 
        return output_sql
 
def create_ext_job(client , extract_prf_id, region_code , login_name ):
    if region_code == 'NA':
        job_region_code = NA_ext_job
    elif region_code == 'EU':
        job_region_code = EU_ext_job
    elif region_code == 'FE':
        job_region_code = FE_ext_job
    elif region_code == 'IN':
        job_region_code = IN_ext_job
    elif region_code == 'SA':
        job_region_code = SA_ext_job
    job_region_code['profileId'] = extract_prf_id
    new_job = client.create_job(
        # job = job_region_code
        job = {'__type' : job_region_code["job_type"], # "com.amazon.datanet.model#ExtractJob",
               'type': job_region_code["type"],
               'description': job_region_code["description"],#'%s Auto generated for Profile Id: %s' % (job_name, str(job_profile_id)),
               'freeForm':job_region_code["freeForm"],
               'owner': login_name,
               'partition_keys': [
                   {
                       'partition_type_id': job_region_code["partitionTypeId"],
                       'partition_value': int(job_region_code["partitionValue"])
                   }
               ],
               'group': job_region_code["group"],
               'version_attributes': {
                   'updated_by': login_name
               },
               'timezone': job_region_code["timezone"],
               'priority': job_region_code["priority"],
               'host_group': job_region_code["hostGroup"],
               'profile_id': job_region_code["profileId"],
               'db_user': job_region_code["dbUser"],
               'logical_db': job_region_code["logicalDB"]
               }
        , request_context=request_context_user
    )
    logging.debug(" For above extract, {0} Job ID: {1}" .format(region_code, new_job.job.id))
    return new_job
 
 
def create_dq_profiles(datanet_client, db_name, prod_table, test_table_name, join_columns, login_name):
    output_sql = create_dq_sql(db_name, prod_table, test_table_name, join_columns, login_name)
 
    #print(output_sql)
    login_name= login_name.lower()
    prod_table_schema = prod_table.split(".")[0]
    prod_table_name = prod_table.split(".")[1]
    test_table_schema = test_table_name.split(".")[0]
    test_table_name = test_table_name.split(".")[1]
    request_context_user.login_name = login_name
    edx_file_template = f"/dss/dwp/data/DQ_{prod_table_name.upper()}_{login_name}_{{JOBRUN_DETAILS}}.txt"
    job_profile_description = f"""DQ profile to compare {prod_table_schema.upper()}.{prod_table_name.upper()} and {test_table_schema.upper()}.{test_table_name.upper()}"""
 
 
    logging.debug("Creating Extract profile")
    create_ext_job_profile_resp = create_ext_job_profile(datanet_client, job_profile_description, output_sql, edx_file_template, login_name)
    create_ext_job_profile_resp_json = CoralRpcEncoder().encode(create_ext_job_profile_resp)
    create_ext_job_profile_resp_dict = json.loads(create_ext_job_profile_resp_json)
    #pprint.pprint(dict(create_ext_job_profile_resp_dict))
 
    if create_ext_job_profile_resp_dict['jobProfile']['id']:
        logging.debug(f"Extract Profile {create_ext_job_profile_resp_dict['jobProfile']['id']} created successfully")
    else:
        logging.error(f"Creation of extract profile failed!!. Exiting")
        logging.debug(output_sql)
        sys.exit(127)
 
    extract_url = f"https://datacentral.a2z.com/dw-platform/servlet/dwp/template/EtlViewExtractJobs.vm/job_profile_id/{create_ext_job_profile_resp.job_profile.id}"
 
    logging.debug("Creating Job for NA")
    create_ext_job(datanet_client, create_ext_job_profile_resp_dict['jobProfile']['id'], 'NA', login_name)
    logging.debug("Creating Job for SA")
    create_ext_job(datanet_client, create_ext_job_profile_resp_dict['jobProfile']['id'], 'SA', login_name)
    logging.debug("Creating Job for FE")
    create_ext_job(datanet_client, create_ext_job_profile_resp_dict['jobProfile']['id'], 'FE', login_name)
    logging.debug("Creating Job for EU")
    create_ext_job(datanet_client, create_ext_job_profile_resp_dict['jobProfile']['id'], 'EU', login_name)
    logging.debug("Creating Job for IN")
    create_ext_job(datanet_client, create_ext_job_profile_resp_dict['jobProfile']['id'], 'IN', login_name)
 
    logging.debug("Sending Email")
    send_html_email(job_profile_description, f"{login_name}@amazon.com", f"EXTRACT URL: {extract_url}")
 
def main():
    #db_name = 'dwrsm010'
    #prod_table = 'BOOKER.D_MP_LEGAL_ENTITY_MAP'
    #test_table_name = 'BOOKER.D_MP_LEGAL_ENTITY_MAP'
    #join_columns = 'marketplace_id'
    #login_name = 'sinparup'
    #datanet_client = ''
    #db_name = input("Enter the Database Name (e.g., DWRSM017): ").strip().lower()
    #prod_table = input("Enter the Production Table (e.g., BOOKER.d_coi_reference_attributes_eu): ").strip()
    #test_table_name = input("Enter the Test Table (e.g., BIC_DDL.d_coi_reference_attributes_eu): ").strip()
    #join_columns = input("Enter the Join Columns (comma-separated, e.g., marketplace_id): ").strip()
    #login_name = input("Enter the Login Name (e.g., sukulma): ").strip().lower()
 
    # Call create_dq_profiles() with the required arguments
    create_dq_profiles(datanet_client, db_name, prod_table, test_table_name, join_columns, login_name)
 
if __name__ == "__main__":
    main()
