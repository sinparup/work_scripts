import os
import subprocess
import requests
import logging
import json
import re
import sys
import datetime
from bdt_content_py_utils.datanetUtilFiles.datanetClient import get_datanet_client
from bdt_content_py_utils.commonUtils.DBConnection import DBConnection
from MaestroDQScript import create_dq_profiles
from MaestroDQScript import create_dq_sql
from MaestroCreateSubscription import create_subscription
from MaestroRunJob import run_jobs_for_ids
from com.amazon.datanet.service.requestcontext import RequestContext
from com.amazon.datanet.service.getjobprofilerequest import GetJobProfileRequest
from pyodinhttp import odin_material_retrieve, odin_retrieve, odin_retrieve_pair
from com.amazon.datanet.service.updatejobprofilerequest import UpdateJobProfileRequest
from coral.coralrpchandler import CoralRpcEncoder
import time
from sync_workspace import run_command, sync_package

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logging.info("Start")
logger = logging.getLogger(__name__)
log_filename = datetime.datetime.now ().strftime ('djs_code_run_%Y%m%d_%H_%M_%S.log')
logging.basicConfig (level = logging.DEBUG,
                     format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                     datefmt = '%a, %d %b %Y %H:%M:%S',
                     filename = '/tmp/' + log_filename,
                     filemode = 'a')
 
# Constants
COOKIE_FILE = os.path.expanduser("~") + '/.midway/cookie'
change_flag = False
#login_name = "sinparup"
#pipeline_path = "/home/sinparup/BDTContentOrdering/BDTContentOrdering/src/BDTContentOrderingCDK"
#test_pipeline_path = "/home/sinparup/BDTContentOrderingTests/BDTContentOrderingTests/src/BDTContentOrderingTests"
#extract_profile_id = "12469421"
#table_name = "BIC_DDL.O_NON_DC_PEND_CUST_SHIPMENTS_MAESTRO"
#join_column = "order_id"
#dbname = "dwrsm017"
 
# instantiate DatanetClient
try:
    #print('Entering get_datanet_client function......')
    datanet_client = get_datanet_client ()
    print('Connected to datanet client......')
except Exception as e:
    logging.error (e)
    sys.exit ("Datanet Client not working properly!!!")
    
def get_job_profile(profile_id, job_type):
    #print('Entering get_job_profile funtion........')
    # Convert profile_id to int if it's a string
    if isinstance(profile_id, str):
        try:
            profile_id = int(profile_id)
        except ValueError:
            logging.error(f"Invalid profile_id: {profile_id}. Must be an integer.")
            return None
    #print('profile_id -> ', profile_id)
    get_request = GetJobProfileRequest(job_profile_id=profile_id, job_type=job_type)
    #print('get_request -> ', get_request)
    response = datanet_client.get_job_profile(get_request)
    #print('get_job_profile response --> ', response)
    return response
    
def enter_folder(folder_name, ignore_case=False):
    # Define characters to replace in folder names for normalization
    replace_chars = {' ': '-', '*': '', ':': '', '(': '', ')': '', '.': '', '/': '_', '[': '', ']': '', ',': ''}
    
    # Process folder name to make it suitable for comparison
    folder_name_processed = folder_name
    #print('folder_name_processed -> ', folder_name_processed)
    for orig_char, repl_char in replace_chars.items():
        folder_name_processed = folder_name_processed.replace(orig_char, repl_char)
    folder_name_processed = folder_name_processed.replace('MaestroBDTContentOrdering---', '')
    folder_name_processed = folder_name_processed.replace('Maestro:BDTContentOrdering - ', '')
    folder_name_processed = folder_name_processed.replace('MENTS_MAESTROBETA', '')
    folder_name_processed = folder_name_processed.replace('.', '')
    folder_name_processed = folder_name_processed.replace(' ', '-')
    folder_name_processed = folder_name_processed.replace('_BETA', '')
    #print('folder_name_processed -> ', folder_name_processed)
    
    # Convert to lowercase for case-insensitive comparison
    folder_name_processed_lower = folder_name_processed.lower()
    #print('folder_name_processed_lower ->> ', folder_name_processed_lower)

    
    # Iterate over items in the current directory
    for item in os.listdir():
        # Check if the processed folder name matches and is a directory
        if os.path.isdir(item) and folder_name_processed_lower in item.lower():
            os.chdir(item)
            #print(f"Entered Folder '{item}'")
            return
    
    # Raise an exception if the folder is not found
    raise Exception(f"Folder '{folder_name}' not found (case-insensitive).")
 
def update_tranf_profile(filename, updated_logic):
    global change_flag
    change_flag = True
    print("Updating SQL file:", filename)
    try:
        with open(filename, 'r') as sql_file:
            original_sql = sql_file.read()
            #print('Original SQL:', original_sql)
        
        with open(filename, 'w') as sql_file:
            sql_file.write(updated_logic)
            print('Updated SQL written to file.')
    except Exception as e:
        print(f"Unable to open file or write SQL. Error: {e}")
 
# Function to update the pragma in the JSON file
def update_pragma_json(file_path, updated_pragma):
    try:
        # Ensure updated_pragma is a dictionary
        updated_pragma_dict = json.loads(updated_pragma)
 
        with open(file_path, 'r') as file:
            data = json.load(file)
 
        data.update(updated_pragma_dict)
 
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
 
        print("JSON file updated successfully.")
 
    except Exception as e:
        print(f"Failed to update JSON file. Error: {e}")
 
def get_rs_conn(db_name):
    RS_MATERIAL_SET = "com.amazon.dw_rs_reporting_admin.keys"
    principal_val, credential_val = odin_retrieve_pair(RS_MATERIAL_SET, material_serial=1)
    #print('principal_val', principal_val)
    #print('credential_val', credential_val)
    cluster_host = {
        "dwrstest": "dw-rs-test.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwmetadata": "dw-bdt-metadata.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm003": "dw-rsm-003.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm014": "dw-rsm-014.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm015": "dw-rsm-015.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm017": "dw-rsm-017.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm010": "dw-rsm-020.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm022": "dw-rsm-022.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm029": "dw-rsm-029.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm005": "dw-rsm-205.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm018": "dw-rsm-218.cv63a8urwqge.us-east-1.redshift.amazonaws.com",
        "dwrsm012": "dw-rsm-412.cv63a8urwqge.us-east-1.redshift.amazonaws.com"
    }
    try:
        rsClient = DBConnection(db_name, cluster_host[db_name], 8192, '')
        rs_conn = rsClient.create_rs_db_connection(RS_MATERIAL_SET)
        return rs_conn
    except Exception as e:
        logging.error("RS connection failed with exception:", e)
 
import logging
import time

def create_rs_test_table(prod_table_name, dbname, rs_conn):
    schema, table_name = prod_table_name.split('.')
    test_table_name = f"BIC_DDL.{table_name}_BETA"
    SQL_getRedshiftTableSchema = f"CREATE TABLE {test_table_name} (LIKE {prod_table_name});"
    
    try:
        rs_conn.query(SQL_getRedshiftTableSchema)
        rs_conn.commit()
        logging.info("Created Test Table Successfully!!")
        time.sleep(10)
    except Exception as query_err:
        logging.error(f"Failed to create table (it might already exist): {query_err}")
        # Continue with the next steps even if table creation fails
    
    grant_access_query = f"""
    GRANT ALL ON {test_table_name} TO "CDO:amzn:cdo:datanet-dbuser:aztec_long_npe";
    GRANT ALL ON {test_table_name} TO "CDO:amzn:cdo:datanet-dbuser:loader_npe";
    GRANT ALL ON {test_table_name} TO "CDO:amzn:cdo:datanet-dbuser:booker_test_readonly";
    """
    try:
        rs_conn.query(grant_access_query)
        logging.info("Granted access to the test table.")
    except Exception as e:
        logging.error(f"Failed to grant access: {e}")
    
    return test_table_name


 

def run_shell_script(table_name, test_table_name, beta_provider, prod_provider):
    # Run the shell script
    result = subprocess.run(
        ["./MaestroAndesTableCreation.sh", table_name, test_table_name, beta_provider, prod_provider],
        capture_output=True,
        text=True
    )

    # Print the script output (for debugging purposes)
    print(result.stdout)

    # Parse the output to find table name and version
    output_lines = result.stdout.splitlines()
    created_table_name = None
    created_version_number = None
    
    for line in output_lines:
        if line.startswith("TableName:"):
            created_table_name = line.split(":")[1].strip()
        elif line.startswith("VersionNumber:"):
            created_version_number = line.split(":")[1].strip()

    return created_table_name, created_version_number


def create_json_in_data_component_folder(table_name, profile_folder_path):
    data_folder = None
    for root, dirs, files in os.walk(os.getcwd()):
        for dir_name in dirs:
            if "data" in dir_name:
                data_folder = os.path.join(root, dir_name)
                break
        if data_folder:
            break
    
    if data_folder:
        component_folder = os.path.join(data_folder, "component")
        if not os.path.exists(component_folder):
            os.makedirs(component_folder)
        
        json_file_name = f"run-beta-validations-{table_name}.json"
        json_file_path = os.path.join(component_folder, json_file_name)
        
        # Get user inputs for ARNs and dataset date
        
        test_job_arn = f"arn:cdo:bdt::maestro:datapipeline/BDTContentCustomerAnalytics/stage/beta/platform/datanet/profile/{profile_folder_path}"
 
        dataset_date = input("Enter the dataset date (YYYY-MM-DD): ")
        
        # Construct JSON data
        json_data = {
    "Name": "Runnning datanet jobs and validating its deployments in Beta Stage of BDTContentOrdering Pipeline",
    "Service": "MaestroIntegrationsTest",
    "serviceType": "CORAL",
    "Method": "POST",
    "Config": {
      "datapipelineName": "BDTContentOrdering",
      "successStatus": "SUCCESS",
      "stage": "Beta"
    },
    "TestCases": [
      {
        "TestUnits": [
          {
            "Name": "Test Extract Profile Success Test Case",
            "Resource": "arn:cdo:bdt::maestro:datapipeline/BDTContentOrdering/stage/beta/platform/datanet/profile/Maestro-Testing-Prod-Copy-Transform-for-preparing-latest-snapshot-of-O_NON_DC_PEND_CUST_SHIPMENTS/job/REGION_EU_ACBUK",
            "Input": {
              "Body": {
                "datapipelineName": "${dynamic#getDatapipelineName}",
                "datasetDate": "2024-08-10"
              }
            },
            "Output": {
              "ResponseCode": "200",
              "Validators": {
                "status": "equalTo(\"SUCCESS\")"
              }
            }
          },
          {
            "Name": "Test Extract Profile Success Row Count Test Case",
            "Resource": "arn:cdo:bdt::maestro:datapipeline/BDTContentOrdering/stage/beta/platform/datanet/profile/Maestro-Testing-Prod-Copy-Transform-for-preparing-latest-snapshot-of-O_NON_DC_PEND_CUST_SHIPMENTS/job/REGION_EU_ACBUK",
            "Input": {
              "Body": {
                "datapipelineName": "${dynamic#getDatapipelineName}",
                "datasetDate": "2024-08-10"
              }
            },
            "Output": {
              "ResponseCode": "200",
              "Validators": {
                "rowsReturned": "greaterThan(0)"
              }
            }
          },
          {
            "Name": "DQ Extract for A-B and B-A Count Test Case",
            "Resource": "arn:cdo:bdt::maestro:datapipeline/BDTContentOrdering/stage/beta/platform/datanet/profile/Maestro-Pipeline-Sample-DQ-Profile/job/REGION_EU_EU",
            "Input": {
              "Body": {
                "datasetDate": "2024-08-10"
              }
            },
            "Output": {
              "ResponseCode": "200",
              "Validators": {
                "rowsReturned": "0"
              }
            }
          }
        ]
      }
    ]
  }
        
        with open(json_file_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        
        print(f"Created JSON file: {json_file_path}")
        
        return json_file_name
    else:
        print("Data folder not found.")
        return dataset_date
 
def update_runner_json(table_name):
    data_folder = None
    for root, dirs, files in os.walk(os.getcwd()):
        for dir_name in dirs:
            if "data" in dir_name:
                data_folder = os.path.join(root, dir_name)
                break
        if data_folder:
            break
    
    if data_folder:
        workflow_folder = os.path.join(data_folder, "workflow")
        if not os.path.exists(workflow_folder):
            os.makedirs(workflow_folder)
        
        runner_json_path = os.path.join(workflow_folder, "runner.json")
        
        if os.path.exists(runner_json_path):
            with open(runner_json_path, 'r') as runner_file:
                runner_data = json.load(runner_file)
        else:
            # If runner.json doesn't exist, initialize an empty dictionary
            runner_data = {}
        
        # Update or initialize the datapipeline-hydra-beta section
        if "datapipeline-hydra-beta" not in runner_data:
            runner_data["datapipeline-hydra-beta"] = []
        
        # Add the new entry to the list under datapipeline-hydra-beta
        new_entry = f"data/component/run-beta-validations-{table_name}.json"
        if new_entry not in runner_data["datapipeline-hydra-beta"]:
            runner_data["datapipeline-hydra-beta"].append(new_entry)
        
        with open(runner_json_path, 'w') as runner_file:
            json.dump(runner_data, runner_file, indent=4)
        
        print(f"Updated runner.json")
    else:
        print("Data folder not found.")
 
def call_run_job_script(job_ids_str):
    # Convert the comma-separated job IDs string into a list of integers
    job_ids = [int(id.strip()) for id in job_ids_str.split(',')]

    # Get the current date range
    day_range = 10  # Example range in days
    start_date = datetime.date.today() - datetime.timedelta(days=day_range)
    end_date = datetime.date.today() + datetime.timedelta(days=day_range)
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    # Print statements for debugging
    #print("Job IDs List:", job_ids)
    #print("Job IDs String for Command:", job_ids_str)
    #print("Start Date:", start_date_str)
    #print("End Date:", end_date_str)

    # Call the run_jobs_for_ids function from MaestroRunJob.py
    run_jobs_for_ids(datanet_client, job_ids, start_date_str, end_date_str)

def sandbox_command(pipeline_path):
    print(f"Entering into pipeline path to run sandbox command", pipeline_path)
    os.chdir(pipeline_path)
    print(os.getcwd())
    print(f"Running git status Command")
    run_command(f"git status")
    print(f"Running Sandbox Setup Commands")
    run_command(f"nvm use 16")
    run_command(f"npm install -g aws-cdk")
    print(f"Running Sandbox Deploy Command...")
    #run_command("bdtmaestro sandbox-deploy")
    #print(f"Running Sandbox Command...")
    #run_command("bdtmaestro onboarding")
 
def generate_cr(path):
    print(f"Generating CR...")
    os.chdir(path)
    print(os.getcwd())
    run_command("git add .")
    commit_statement = input("Enter the CR Commit Message: ")
    run_command(f"git commit -m \"{commit_statement}\"")
    run_command("cr")
 
def run_command(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
 
def get_multiline_input(prompt):
    print(prompt)
    lines = []
    while True:
        line = input()
        if line.strip().upper() == "END_SQL":
            break
        lines.append(line)
    return "\n".join(lines)
 
# Main function
def main():
    print(os.getcwd())
    login_name = 'sinparup'
    pipeline_path = input("Enter local build path of package: ")
    test_pipeline_path = input("Enter local build path of integration test package: ")
    extract_profile_id = int(input("Enter the Extract Profile ID: "))
    table_name = input("Enter the Prod Table name: ")
    join_column = input("Enter Join columns for the DQ Profile: ")
    is_redshift_or_andes = input("Enter 1 if you want to create parallel pipeline in Redshift, or enter 2 if you want to create parallel pipeline in Andes: ")
    
    if is_redshift_or_andes == '1':
        dbname = input("Enter the Cluster in which test table needs to be created: ")
        print('Creating Test Table........')
        test_table_name = f"BIC_DDL.{table_name}_BETA"
        #test_rs_table_name = create_rs_test_table(table_name, dbname, get_rs_conn(dbname))
        print(f"Test table Created Successfully!!")

    if is_redshift_or_andes == '2':
        table_name = table_name.split('.')[-1]
        TEST_ANDES_TABLE_NAME = table_name + '_BETA'
        print('Creating Test Andes Table........')
        # Call the shell script with arguments
        created_table_name, created_version_number = run_shell_script(table_name, TEST_ANDES_TABLE_NAME, 'bic_ddl', 'bic_ddl')
        version_number = created_version_number
        #print(test_table_name, version_number)
        print('Creating Subscription on the Test Table........')
        subscription_target_id = input("Enter the Subscription Target ID - ")
        create_subscription(subscription_target_id, 'bic_ddl', TEST_ANDES_TABLE_NAME, version_number)
    

    os.chdir(pipeline_path)
    print(os.getcwd())
    
    #with open("inputProfiles.csv", "w"):
    #    pass
    def_folder = None
    for root, dirs, files in os.walk(os.getcwd()):
        for dir_name in dirs:
            if "def" in dir_name and not "example" in dir_name:
                #print('Entered Definition Folder........')
                def_folder = os.path.join(root, dir_name)
                break
        if def_folder:
            break
 
    if not def_folder:
        raise Exception("Folder containing 'def' not found........")
 
    parts = def_folder.split("/")
    extracted = parts[-1]
    #print(os.getcwd())
    profile_id = extract_profile_id
    profile_type = 'TRANSFORM'

    profile_data = get_job_profile(profile_id, profile_type)
 
    if profile_data is None:
        logging.error("Failed to fetch profile data")
        return
 
    try:
        job_profile = profile_data.job_profile
 
        profile_description = job_profile.description
        output_template = job_profile.output.file_template
 
        #print(f"Profile Description: {profile_description}")
        #print(f"Output Template: {output_template}")
 
        folder_path = os.getcwd()
        #print(f"Current Working Directory: {folder_path}")
 
    except AttributeError as e:
        logging.error(f"Error accessing job profile attributes: {e}")
        return
 
    job_profile = profile_data.job_profile
    profile_description = job_profile.description
    enter_folder(extracted)
    enter_folder(profile_description)
    profile_folder_path = os.getcwd()

    '''
    # Loop for .sql files
    for root, dirs, files in os.walk(profile_folder_path):
        for file_name in files:
            if file_name.endswith(".sql"):
                print('File name : ', file_name)
                file_path = os.path.join(root, file_name)
                print("File Path : ", file_path)
                print('Updating Transform SQL File......')
                update_tranf_profile(file_path, updated_logic)
 
    # Loop for .json files
    if pragma_change == 'Y':
        for root, dirs, files in os.walk(profile_folder_path):
            if root == profile_folder_path:  # Check if the current directory is the main folder
                print('json profile_folder_path -> ', profile_folder_path)
                for file_name in files:
                    if file_name.endswith(".json"):
                        print('File name : ', file_name)
                        file_path = os.path.join(root, file_name)
                        print("File Path : ", file_path)
                        print('Updating Transform JSON File with new Pragma......')
                        update_pragma_json(file_path, updated_pragma)
    '''

    test_table_name = table_name + '_BETA'
    is_dq_profile_existing = input("Is DQ Profile existing in Maestro Package already? (N/Y): ")
    if is_dq_profile_existing == 'N':
        print('Creating DQ Profile.......')
        create_dq_profiles(datanet_client, dbname, table_name, test_table_name, join_column, login_name)
    elif is_dq_profile_existing == 'Y':
        dq_profile_id = input("Enter the DQ Extract Profile ID: ")
        print('Getting DQ SQL Logic........')
        dbname = 'dwrsm017'
        updated_dq_logic = create_dq_sql(dbname, table_name, test_table_name, join_column, login_name)
 
        os.chdir(pipeline_path)
        print(os.getcwd())
        def_folder = None
        for root, dirs, files in os.walk(os.getcwd()):
            for dir_name in dirs:
                print('dir_name -> ', dir_name)
                if "def" in dir_name and not "example" in dir_name:
                    #print('Entered Definition Folder........')
                    def_folder = os.path.join(root, dir_name)
                    break
            if def_folder:
                break
 
        if not def_folder:
            raise Exception("Folder containing 'def' not found........")
 
        parts = def_folder.split("/")
        extracted = parts[-1]
        #print(os.getcwd())
        profile_type = 'TRANSFORM'
        dq_profile_data = get_job_profile(dq_profile_id, profile_type)
        #print("dq_profile_data -> ", dq_profile_data)
 
        if dq_profile_data is None:
            logging.error("Failed to fetch profile data")
            return
 
        dq_profile_data = get_job_profile(dq_profile_id, profile_type)
        dq_job_profile = dq_profile_data.job_profile
        dq_profile_description = dq_job_profile.description
        #dq_output_template = dq_profile_data.output.file_template
 
        #print(f"Profile Description: {dq_profile_description}")
        #print(f"Output Template: {dq_output_template}")

        '''
        job_profile = profile_data.job_profile
 
        profile_description = job_profile.description
        output_template = job_profile.output.file_template
 
        print(f"Profile Description: {profile_description}")
        print(f"Output Template: {output_template}")
 
        folder_path = os.getcwd()
        print(f"Current Working Directory: {folder_path}")

        '''

        folder_path = os.getcwd()
        #print(f"Current Working Directory: {folder_path}")
        enter_folder(extracted)
        enter_folder(dq_profile_description)
        folder_path = os.getcwd()
        #print("folder_path -> ", folder_path)
        print('Updating DQ SQL in Package....')
 
        # Loop for .sql files
        for root, dirs, files in os.walk(folder_path):
            for file_name in files:
                if file_name.endswith(".sql"):
                    #print('File name : ', file_name)
                    file_path = os.path.join(root, file_name)
                    #print("File Path : ", file_path)
                    #print('Updating Transform SQL File......')
                    update_tranf_profile(file_path, updated_dq_logic)  
    # Entering Test Package
    print('Entering Integration Test Package.....')
    os.chdir(test_pipeline_path)
    #print(os.getcwd())
    
    # Create JSON file in data/component folder
    print('Creating integration test suites.......')
    json_file_name = create_json_in_data_component_folder(table_name, profile_folder_path)
 
    if json_file_name:
        print('Updating runner json file.....')
        #Update runner.json in data/workflow folder
        update_runner_json(table_name)
 
    if change_flag:
        sandbox_command(pipeline_path)
        print('Running Job...')
        job_ids_input = input("Enter the Job IDs (separated by comma) : ")
        call_run_job_script(job_ids_input)
        cr_needed = input("Do you want to generate a CR now? - Y/N ")
        
        if cr_needed == 'Y':
            print("Generating CR Main Package.......")
            generate_cr(pipeline_path)
            print("Generating CR for Integration Test Package.......")
            generate_cr(test_pipeline_path)
 
        print(" Exiting the script :) ")
 
    os.chdir(pipeline_path)
    #session.close()
 
if __name__ == "__main__":
    main()
    '''
    /home/sinparup/BDTContentShipping/BDTContentShipping/src/BDTContentShippingCDK
    /home/sinparup/BDTContentShippingTests/BDTContentShippingTests/src/BDTContentShippingTests
    Prod Extract - 12563016
    Prod Table - BIC_DDL.O_NON_DC_PEND_CUST_SHIPS_MAESTRO
    Join Column - order_id
    Cluster - dwrsm017
    DQ Profile - 12562960
    Job Ids - 26917433,26917434
    '''
