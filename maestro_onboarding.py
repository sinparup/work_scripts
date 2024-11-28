import os,subprocess,requests
import glob
import json,re,logging as log
from bdt_content_py_utils.datanetUtilFiles.datanetClient import get_datanet_client
from com.amazon.datanet.service.requestcontext import RequestContext
from com.amazon.datanet.service.getjobprofilerequest import GetJobProfileRequest
from com.amazon.datanet.service.updatejobprofilerequest import UpdateJobProfileRequest
from coral.coralrpchandler import CoralRpcEncoder
 
COOKIE_FILE = os.path.expanduser("~") + '/.midway/cookie'
datanet_client = get_datanet_client()
 
def request_follow_redirects(session, url, headers, max_hops=10):
    if max_hops < 0:
        return False
    max_hops -= 1
    response = session.get(url, headers=headers, allow_redirects=False)
    if response.status_code == 302 or response.status_code == 307:
        return request_follow_redirects(session, response.headers['Location'], headers)
    else:
        return response
 
def sentry_init():
    session = requests.Session()
    session.allow_redirects = False
    session.max_redirects = 5
    session.verify = "/etc/pki/tls/certs/ca-bundle.crt"
    response = session.post('https://sentry.amazon.com/sentry-braveheart?value=1')
    if not response.ok:
        print("sentry error")
        exit(0)
    fd = open(COOKIE_FILE)
    for line in fd:
        elem = re.sub(r'^#HttpOnly_', '', line.rstrip()).split()
        if len(elem) == 7:
            cookie_obj = requests.cookies.create_cookie(domain=elem[0], name=elem[5], value=elem[6])
            session.cookies.set_cookie(cookie_obj)
    return session
 
 
def sentry_get(session, url):
    headers = {'Accept': 'application/vnd.dryad.v1+json'}
    return request_follow_redirects(session, url, headers=headers)
 
def sentry_put(session, url, data):
    headers = {'Accept': 'application/vnd.dryad.v1+json', 'Content-Type': 'application/vnd.dryad.v1+json'}
    return session.put(url, json=data, headers=headers)
 
def sentry_get_config(session, url):
    headers = {'Accept': 'application/table_version_summmary+json'}
    return request_follow_redirects(session, url, headers=headers)
 
def run_cmd(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occuring: {e}")
 
def enter_folder(folder_name, ignore_case=False):
    if ignore_case:
        folder_name_new = folder_name.lower().replace(' ','-').replace('*','').replace(':','').rstrip('-').replace('(','').replace(')','').replace('.','').replace('/','_').replace('[','').replace(']','').replace(',','').replace(';','').replace('{','').replace('}','')
        for item in os.listdir():
            if item.lower() == folder_name_new and os.path.isdir(item):
                os.chdir(item)
                return
        raise Exception(f"Folder '{folder_name_new}' not found (case-insensitive).")
    else:
        folder_name_new = folder_name.replace(' ','-').replace('*','').replace(':','').rstrip('-').replace('(','').replace(')','').replace('.','').replace('/','_').replace('[','').replace(']','').replace(',','').replace(';','').replace('{','').replace('}','')
        if os.path.isdir(folder_name_new):
            os.chdir(folder_name_new)
        else:
            raise Exception(f"Folder '{folder_name_new}' not found.")
 
table_freeform_list = []        
def update_andes_profile(filename):
    print("Opening the file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if '$[Stage]' not in data['definition']['description']:
                foldername =  re.split('[/.]',filename)[-2]
                finaldesc = re.split( '[-]' , data['definition']['description'] )[0] + '- ' + foldername + '$[Stage]'
                data['definition']['description'] = finaldesc
                print('description is updated to ', data['definition']['description'])
            for input in data['inputs']:
                if 'EDX' in input['type']:
                    if '$[' not in input['value']:
                        input['value'] = input['value'] + '$[Stage]'
                        print('input is updated to ',input['value'])  
                elif 'FILE_TEMPLATE' in input['type']:   
                    if "BDTAnalyticsCuration" in filename:
                        index_append = input['value'].find('{')
                        if '$[' not in input['value']:
                            input['value'] = input['value'][:index_append] + '$[FPStage]_' + input['value'][index_append:]
                            print('input is updated to ',input['value'])
                    else:
                        index_append = input['value'].find('.')
                        if '$[' not in input['value']:
                            input['value'] = input['value'][:index_append] + '$[Stage]' + input['value'][index_append:]
                            print('input is updated to ',input['value'])
            for output in data['outputs']:
                if '$[' not in output['value']:
                    table_name=output['value'].split('.')[1]
                    provider_name=output['value'].split('.')[0]
                    split_table_abbreviation = re.split('[._]',output['value'])
                    table_abbreviation_list = []
                    for words in split_table_abbreviation:
                        table_abbreviation_list += words[0]
                    table_abbreviation = ''.join(table_abbreviation_list)[1:]
                    counter = 0
                    path=os.getcwd()
                    os.chdir('..')
                    cfg_files = glob.glob('*.cfg')
                    cfg_file = cfg_files[0]
                    with open(cfg_file, 'r') as file:
                        config_file = file.read()
                        while "."+table_abbreviation+"PartitionScheme" in config_file:
                            if counter == 0:
                                table_abbreviation = table_abbreviation + output['value'][-3:]
                            if counter>0:
                                table_abbreviation = table_abbreviation + str(counter)
                            counter += 1
                    os.chdir(path)
                    while table_abbreviation in table_freeform_list:
                        if counter == 0:
                            table_abbreviation = table_abbreviation + output['value'][-3:]
                        if counter > 0:
                            table_abbreviation = table_abbreviation + str(counter)
                        counter += 1
                    table_freeform_list.append(table_abbreviation)
 
                    if "BDTAnalyticsCuration" in filename and "f708ebea-a45b-4f71-ab33-3ba65f3919a8" in output['value']:
                        output['value'] = output['value'].replace("f708ebea-a45b-4f71-ab33-3ba65f3919a8",'$[BDTAnalyticsProvider]')
                    elif 'BOOKER_SECURE' in output['value']:
                        output['value'] = output['value'].replace('BOOKER_SECURE','$[ProviderNameSecure]') + '$[Stage]'
                    elif 'booker_secure' in output['value']:
                        output['value'] = output['value'].replace('booker_secure','$[ProviderSecure]') + '$[Stage]'
                    elif 'BOOKER' in output['value']:
                        output['value'] = output['value'].replace('BOOKER','$[ProviderName]') + '$[Stage]'
                    elif 'booker' in output['value']:
                        output['value'] = output['value'].replace('booker','$[Provider]') + '$[Stage]'
                    elif 'BIC_CAT_DDL' in output['value']:
                        output['value'] = output['value'].replace('BIC_CAT_DDL','$[ProviderName2]') + '$[Stage]'
                    else:
                        ind=output['value'].index('.')
                        st=output['value'][:ind]
                        output['value'] = output['value'].replace(st,'$[Provider]') + '$[Stage]'
                    print('output is updated to ', output['value'])
            if '$[' not in data['definition']['profileData']['partitionSchemeName']:
                data['definition']['profileData']['partitionSchemeName'] = '$[' + table_abbreviation + 'PartitionScheme]'
                data['definition']['profileData']['tableMajorVersion'] = '$[' + table_abbreviation + 'VersionNumber]'
                if "BDTAnalyticsCuration" in filename: 
                    beta_details="368e255c-c788-4499-9a6e-c5c9c5e40c68"+ "/tables/" + table_name
                else:
                    beta_details="bic_ddl"+ "/tables/" + table_name
                session = sentry_init()
                res = sentry_get_config(session, "https://andes-service-iad.iad.proxy.amazon.com/v2/providers/" + beta_details + "/versions/summary")
                print(res)
                if res.status_code != 200:
                    print("Getting table version summary failed ")
                    return
                prof = json.loads(res.text)
                partition_scheme_beta='"' + prof["recommended"]["legacyPartitionSchemeName"] + '"'
                version_number_beta='"' + str(prof["recommended"]["versionNumber"]) + '"'
 
                res1 = sentry_get_config(session, "https://andes-service-iad.iad.proxy.amazon.com/v2/providers/" + provider_name + "/tables/" + table_name + "/versions/summary")
                print(res1)
                if res1.status_code != 200:
                    print("Getting table version summary failed ")
                    return
                prof = json.loads(res1.text)
                partition_scheme_prod='"' + prof["recommended"]["legacyPartitionSchemeName"] + '"'
                version_number_prod='"' + str(prof["recommended"]["versionNumber"]) + '"'
                os.chdir('..')
                cfg_files = glob.glob('*.cfg')
                cfg_file = cfg_files[0]
                with open(cfg_file, 'a') as config_file:
                    config_file.write('\n' + "#"+table_name+"\n")
                    config_file.write("beta.*." + table_abbreviation + "PartitionScheme" + "=" +partition_scheme_beta+";"+ '\n')
                    config_file.write("prod.*." + table_abbreviation + "PartitionScheme" + "=" +partition_scheme_prod+";"+ '\n')
                    config_file.write("beta.*." + table_abbreviation + "VersionNumber" + "=" +version_number_beta+";"+ '\n')
                    config_file.write("prod.*." + table_abbreviation + "VersionNumber" + "=" + version_number_prod+";"+ '\n')
                os.chdir(path)
 
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except OSError:
        print("Unable to open File, did not change anything")
 
 
def update_andes_job(filename):
    print("Updating the job file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if '$[' not in data['definition']['group']:
                if "BDTAnalyticsCuration" in filename and "BDT-ANALYTICS-PROD" in data['definition']['group']:
                    data['definition']['group'] = '$[Group]'
                elif "BDTAnalyticsCuration" in filename and "DW" in data['definition']['group']:
                    data['definition']['group'] = '$[DWGroup]'
                elif "BDTAnalyticsCuration" in filename and "DWRS" in data['definition']['group']:
                    data['definition']['group'] = '$[DWRGroup]'
                elif 'DWRS' in data['definition']['group']:
                    data['definition']['group'] = '$[RSgroup]'
                elif 'DW' in data['definition']['group']:
                    data['definition']['group'] = '$[Group]'
                else:
                    data['definition']['group'] = data['definition']['group']
                print('group is updated to ', data['definition']['group'])     
            if 'schedule' in data['definition']:
                if 'DAILY' in data['definition']['schedule']['type']:
                    if len(data['definition']['schedule']['dayPreferences']) == 7:
                        data['schedule'] = '$[ScheduledDaily]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')  
                elif 'time' in data['definition']['schedule'] and 'INTRADAY' in data['definition']['schedule']['type']:
                    time_len=len(data['definition']['schedule']['time'])
                    if time_len==24:
                        data['schedule'] = '$[ScheduledHourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')  
                    elif time_len==12:
                        data['schedule'] = '$[Scheduled2Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')                          
                    elif time_len==6:
                        data['schedule'] = '$[Scheduled4Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted') 
                    elif time_len==3:
                        data['schedule'] = '$[Scheduled8Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')                                                 
                else:
                    data['schedule']='$[NotScheduled]'
                    data['definition'].pop('schedule')
                    print('Legacy schedule deleted')                 
            if 'owner' not in data['definition']:
                data['definition']['owner'] = '$[Owner]'
                print('Owner added')     
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except OSError:
        print("Unable to open File, did not change anything")
 
def update_rs_profile(filename):
    print("Opening the file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if '$[Stage]' not in data['definition']['description']:
                foldername =  re.split('[/.]',filename)[-2]
                finaldesc = re.split( '[-]' , data['definition']['description'] )[0] + '- ' + foldername + '$[Stage]'
                data['definition']['description'] = finaldesc
                print('description is updated to ', data['definition']['description'])
            if '$[' not in data['definition']['logicalDb']:
                if "BDTAnalyticsCuration" not in filename:
                    if 'L-DW-RSM-CATALOG' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[RSCatalogLoadLogicalDB]'
                    elif 'L-DW-RS-LOAD' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[LDWRSLoadLogicalDB]'
                    elif 'L-DW-RS-GENERIC' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[RSGenericLoadLogicalDB]'
                    elif 'L-DW-RS-CLICKSTREAM' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[RSClickstreamLoadLogicalDB]'
                    elif 'L-DW-RS-INTEG' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[RSIntegLoadLogicalDB]'
                    elif 'L-DW-RS-TRANSACTIONS' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[WHASIExtLogicalDB]'
                    elif 'L-DW-RS-INTRADAY' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[RSIntradayLogicalDB]'   
                    elif 'DWMETADATA' in data['definition']['logicalDb']:
                        data['definition']['logicalDb'] = '$[METADATALoadLogicalDB]'
                    else:
                        data['definition']['logicalDb'] = data['definition']['logicalDb']         
                    print('logicalDb is updated to ', data['definition']['logicalDb'])    
            if 'loadOption' in data['definition']:
                if 'mergeOption' in data['definition']['loadOption']:
                    if data['definition']['loadOption']['mergeOption']=="":
                        data['definition']['loadOption']['mergeOption']='UPSERT'
                    print("Updated to ",data['definition']['loadOption']['mergeOption'])
            for input in data['inputs']:
                if 'EDX' in input['type']:
                    if '$[' not in input['value']:
                        input['value'] = input['value'] + '$[Stage]'
                        print('input is updated to ',input['value'])  
                elif 'FILE_TEMPLATE' in input['type']:               
                    if "BDTAnalyticsCuration" in filename:
                        index_append = input['value'].find('{')
                        if '$[' not in input['value']:
                            input['value'] = input['value'][:index_append] + '$[FPStage]_' + input['value'][index_append:]
                            print('input is updated to ',input['value'])
                    else:
                        index_append = input['value'].find('.')
                        if '$[' not in input['value']:
                            input['value'] = input['value'][:index_append] + '$[Stage]' + input['value'][index_append:]
                            print('input is updated to ',input['value'])
            for output in data['outputs']:
                if '$[' not in output['value']:
                    if "BDTAnalyticsCuration" in filename and "f708ebea-a45b-4f71-ab33-3ba65f3919a8" in output['value']:
                        output['value'] = output['value'].replace("f708ebea-a45b-4f71-ab33-3ba65f3919a8",'$[BDTAnalyticsProvider]')
                    elif 'BOOKER_SECURE' in output['value']:
                        output['value'] = output['value'].replace('BOOKER_SECURE','$[ProviderNameSecure]') 
                    elif 'booker_secure' in output['value']:
                        output['value'] = output['value'].replace('booker_secure','$[ProviderSecure]') 
                    elif 'BOOKER' in output['value']:
                        output['value'] = output['value'].replace('BOOKER','$[ProviderName]') 
                    elif 'booker' in output['value']:
                        output['value'] = output['value'].replace('booker','$[Provider]') 
                    elif 'BIC_CAT_DDL' in output['value']:
                        output['value'] = output['value'].replace('BIC_CAT_DDL','$[ProviderName2]') 
                    else:
                        ind=output['value'].index('.')
                        st=output['value'][:ind]
                        output['value'] = output['value'].replace(st,'$[Provider]') 
                    print('output is updated to ', output['value'])
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except:
        print("Unable to open File, did not change anything")
 
 
def update_rs_job(filename):
    print("Updating the job file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if '$[' not in data['definition']['group']:
                if "BDTAnalyticsCuration" in filename and "BDT-ANALYTICS-PROD" in data['definition']['group']:
                    data['definition']['group'] = '$[Group]'
                elif "BDTAnalyticsCuration" in filename and "DW" in data['definition']['group']:
                    data['definition']['group'] = '$[DWGroup]'
                elif "BDTAnalyticsCuration" in filename and "DWRS" in data['definition']['group']:
                    data['definition']['group'] = '$[DWRGroup]'
                elif 'DWRS' in data['definition']['group']:
                    data['definition']['group'] = '$[RSgroup]'
                elif 'DW' in data['definition']['group']:
                    data['definition']['group'] = '$[Group]'
                else:
                    data['definition']['group'] = data['definition']['group']
                print('group is updated to ', data['definition']['group']) 
            if '$[' not in data['definition']['dbUser']:
                if "BDTAnalyticsCuration" not in filename:
                    if 'loader' in data['definition']['dbUser']:
                        data['definition']['dbUser'] = '$[RSLoadDbUser]'
                    else:
                        data['definition']['dbUser'] = data['definition']['dbUser']
                    print('dbUser is updated to ', data['definition']['dbUser'])
            if 'schedule' in data['definition']:
                if 'DAILY' in data['definition']['schedule']['type']:
                    if len(data['definition']['schedule']['dayPreferences']) == 7:
                        data['schedule'] = '$[ScheduledDaily]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')  
                elif 'time' in data['definition']['schedule'] and 'INTRADAY' in data['definition']['schedule']['type']:
                    time_len=len(data['definition']['schedule']['time'])
                    if time_len==24:
                        data['schedule'] = '$[ScheduledHourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')  
                    elif time_len==12:
                        data['schedule'] = '$[Scheduled2Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')                          
                    elif time_len==6:
                        data['schedule'] = '$[Scheduled4Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')    
                    elif time_len==3:
                        data['schedule'] = '$[Scheduled8Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')                                                 
                else:
                    data['schedule']='$[NotScheduled]'
                    data['definition'].pop('schedule')
                    print('Legacy schedule deleted')  
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except OSError:
        print("Unable to open File, did not change anything")
        
def update_tranf_job(filename):
    print("Updating the job file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if '$[' not in data['definition']['group']:
                if "BDTAnalyticsCuration" in filename and "BDT-ANALYTICS-PROD" in data['definition']['group']:
                    data['definition']['group'] = '$[Group]'
                elif "BDTAnalyticsCuration" in filename and "DW" in data['definition']['group']:
                    data['definition']['group'] = '$[DWGroup]'
                elif "BDTAnalyticsCuration" in filename and "DWRS" in data['definition']['group']:
                    data['definition']['group'] = '$[DWRGroup]'
                elif 'DWRS' in data['definition']['group']:
                    data['definition']['group'] = '$[RSgroup]'
                elif 'DW' in data['definition']['group']:
                    data['definition']['group'] = '$[Group]'
                else:
                    data['definition']['group'] = data['definition']['group']
                print('group is updated to ', data['definition']['group']) 
            if '$[' not in data['definition']['dbUser']:
                if "BDTAnalyticsCuration" not in filename:
                    if 'loader' in data['definition']['dbUser']:
                        data['definition']['dbUser'] = '$[RSLoadDbUser]'
                    elif 'aztec_long_npe' in data['definition']['dbUser']:
                        data['definition']['dbUser'] = '$[RSExtDbUser]'
                    elif 'booker_prod_readonly' in data['definition']['dbUser']:
                        data['definition']['dbUser'] = '$[RSExtDbUser2]'
                    elif 'dw_intraday_user_npe' in data['definition']['dbUser']:
                        data['definition']['dbUser'] = '$[RSExtDbUser3]'
                    else:
                        data['definition']['dbUser'] = data['definition']['dbUser']
                    print('dbUser is updated to ', data['definition']['dbUser'])         
            if 'schedule' in data['definition']:
                if 'DAILY' in data['definition']['schedule']['type']:
                    if len(data['definition']['schedule']['dayPreferences']) == 7:
                        data['schedule'] = '$[ScheduledDaily]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')  
                elif 'time' in data['definition']['schedule'] and 'INTRADAY' in data['definition']['schedule']['type']:
                    time_len=len(data['definition']['schedule']['time'])
                    if time_len==24:
                        data['schedule'] = '$[ScheduledHourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')  
                    elif time_len==12:
                        data['schedule'] = '$[Scheduled2Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')                          
                    elif time_len==6:
                        data['schedule'] = '$[Scheduled4Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted') 
                    elif time_len==3:
                        data['schedule'] = '$[Scheduled8Hourly]'
                        print('new schedule is added with value ', data['schedule'])
                        data['definition'].pop('schedule')
                        print('Legacy schedule deleted')                                                    
                else:
                    data['schedule']='$[NotScheduled]'
                    data['definition'].pop('schedule')
                    print('Legacy schedule deleted')  
            if '$[' not in data['definition']['logicalDB']:
                if "BDTAnalyticsCuration" not in filename:
                    if 'L-DW-RSM-CATALOG' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[RSCatalogLoadLogicalDB]'
                    elif 'L-DW-RS-LOAD' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[LDWRSLoadLogicalDB]'
                    elif 'L-DW-RS-GENERIC' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[RSGenericLoadLogicalDB]'
                    elif 'L-DW-RS-CLICKSTREAM' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[RSClickstreamLoadLogicalDB]'
                    elif 'L-DW-RS-INTEG' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[RSIntegLoadLogicalDB]'
                    elif 'L-DW-RS-TRANSACTIONS' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[WHASIExtLogicalDB]'
                    elif 'L-DW-RS-INTRADAY' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[RSIntradayLogicalDB]'   
                    elif 'DWMETADATA' in data['definition']['logicalDB']:
                        data['definition']['logicalDB'] = '$[METADATALoadLogicalDB]'    
                    else:
                        data['definition']['logicalDB'] = data['definition']['logicalDB']                
                    print('logicalDB is updated to ', data['definition']['logicalDB'])
            if 'owner' not in data['definition']:
                if "BDTAnalyticsCuration" in filename:
                    data['definition']['owner'] = '$[Owner]'
                    print('Owner added')                
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except OSError:
        print("Unable to open File, did not change anything")
 
def update_tranf_profile(filename):
    print("Opening the file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if '$[Stage]' not in data['definition']['description']:
                foldername =  re.split('[/.]',filename)[-2]
                finaldesc = re.split( '[-]' , data['definition']['description'] )[0] + '- ' + foldername + '$[Stage]'
                data['definition']['description'] = finaldesc
                print('description is updated to ', data['definition']['description'])
            for output in data['outputs']:
                if "BDTAnalyticsCuration" in filename:
                    index_append = output['value'].find('{')
                    if '$[' not in output['value']:
                        output['value'] = output['value'][:index_append] + '$[FPStage]_' + output['value'][index_append:]
                        print('output is updated to ',output['value'])
                else:
                    index_append = output['value'].find('.')
                    if '$[' not in output['value']:
                        output['value'] = output['value'][:index_append] + '$[Stage]' + output['value'][index_append:]
                        print('output is updated to ',output['value'])
 
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except:
        print("Unable to open File, did not change anything")
 
def update_cradle_job(filename):
    print("Opening the file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            if 'schedule' in data:
                data['schedule'] = '$[Scheduled2Hourly]'
                print('new schedule is added with value ', data['schedule'])
            if 'resource' in data['definition']['jobParameters']['variables']:
                data['definition']['jobParameters']['variables']['resource'] = '$[StandardIOPCluster]'
                print('new resource is added with value ', data['definition']['jobParameters']['variables']['resource'])           
            if 'clusterSize' in data['definition']['jobParameters']['variables']:
                data['definition']['jobParameters']['variables']['clusterSize'] = '$[StandardIOPClusterSize]'
                print('new clusterSize is added with value ', data['definition']['jobParameters']['variables']['clusterSize'])  
 
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except:
        print("Unable to open File, did not change anything")
 
def update_cradle_profile(filename):
    print("Opening the file: ",filename)
    try:
        with open(filename,'r') as json_file:
            data = json.load(json_file)
            for item in data['outputs']:
                table_value = item['table']
                provider_value = item['provider']
                version_value=item['version']
                if '$[Provider]' not in provider_value:
                    if 'BOOKER_SECURE' in provider_value:
                        item['provider']= provider_value.replace('BOOKER_SECURE','$[ProviderNameSecure]')
                    elif 'booker_secure' in provider_value:
                        item['provider'] = provider_value.replace('booker_secure','$[ProviderSecure]')
                    elif 'BOOKER' in provider_value:
                        item['provider'] = provider_value.replace('BOOKER','$[ProviderName]')
                    elif 'booker' in provider_value:
                        item['provider'] = provider_value.replace('booker','$[Provider]')
                    elif 'BIC_CAT_DDL' in provider_value:
                        item['provider'] = provider_value.replace('BIC_CAT_DDL','$[ProviderName2]') 
                    else:
                        ind=provider_value.index('.')
                        st=provider_value[:ind]
                        item['provider'] = provider_value.replace(st,'$[Provider]')
                    print('Provider is updated to ', item['provider'])
                if '$[Stage]' not in table_value:
                    item['table'] = item['table'] + '$[Stage]'
                    print('name is updated to ', item['table']) 
                if '$[OUTPUT_TABLE_VERSION]' not in str(version_value):
                    item['version'] = '$[OUTPUT_TABLE_VERSION]'
                    print('version is updated to ', item['version'])                 
            if 'accountName' in data['definition']:
                data['definition']['accountName'] = '$[CradleAccount]'
                print('account name is updated to ', data['definition']['accountName'])
            if '$[Stage]' not in data['definition']['name']:
                data['definition']['name'] = data['definition']['name'] + '$[Stage]'
                print('name is updated to ', data['definition']['name'])
 
        with open(filename, 'w') as f:
            json.dump(data,f,indent=4)
    except:
        print("Unable to open File, did not change anything")
 
def update_cradle_profile_desc(session, id):
    res = sentry_get(session, "https://dryadservice-na-iad.iad.proxy.amazon.com/profiles/" + id)
    print(res)
    if res.status_code != 200:
        print("Get profile failed " + id)
        return
    profile = json.loads(res.text)
    print('Get profile: {}'.format(profile["profile"]["id"]))
    file_name=profile["profile"]["name"]
    if "Cradle-Andes-Conversion-for-table-" not in file_name:
        file_name=file_name.replace(" ","-")
        profile["profile"]["name"]="Cradle-Andes-Conversion-for-table-"+file_name
        res = sentry_put(session, "https://dryadservice-na-iad.iad.proxy.amazon.com/profiles/" + id,profile["profile"])
        if res.status_code != 200:
            print("Failed Profile: " + id)
        else:
            print(id + " updated successfully")
    else:
        print("\n No need to update profile name for {}".format(profile["profile"]["id"]))
 
def update_profile(
        profile_id,profile_type
    ) -> None:
    try:
        resp = datanet_client.get_job_profile(GetJobProfileRequest(job_profile_id=profile_id, job_type=profile_type,request_context=RequestContext(login_name="sweekyp")))
        raw_json = CoralRpcEncoder().encode(resp)
        job_profile = json.loads(raw_json)
        job_id = job_profile['jobProfile']['id']
        job_og_desc=job_profile['jobProfile']['description']
        if "ANDES_LOAD" in profile_type:
            if "Maestro" in job_og_desc:
                print(f"\nAlready onboarded. No need to update {job_id} Redshift Load Description")   
                return         
            elif "Datanet-Andes-Load-" not in job_og_desc:
                job_new_desc=job_og_desc.replace(" ","-")
                job_prof_desc="Datanet-Andes-Load-"+job_new_desc
                while len(job_prof_desc) >=100:
                    print("Current Profile Description: ",job_prof_desc)
                    job_desc=input("Profile Description exceeds 100 characters, Enter new profile description less than 100 chars : ")
                    job_prof_desc=job_desc
                job_profile['jobProfile']['description']=job_prof_desc
            else:
                print(f"\nNo need to update {job_id} Redshift Load Description")
                return
        elif "LOAD" in profile_type:
            if "Maestro" in job_og_desc:
                print(f"\nAlready onboarded. No need to update {job_id} Redshift Load Description")
                return
            elif "Datanet-Redshift-Load-" not in job_og_desc:
                job_new_desc=job_og_desc.replace(" ","-")
                job_prof_desc="Datanet-Redshift-Load-"+job_new_desc
                while len(job_prof_desc) >=100:
                    print("Current Profile Description: ",job_prof_desc)
                    job_desc=input("Profile Description exceeds 100 characters, Enter new profile description less than 100 chars : ")
                    job_prof_desc=job_desc
                job_profile['jobProfile']['description']=job_prof_desc
            else:
                print(f"\nNo need to update {job_id} Redshift Load Description")
                return
        elif "TRANSFORM" in profile_type:
            if "Maestro" in job_og_desc:
                print(f"\nAlready onboarded. No need to update {job_id} Redshift Load Description")      
                return      
            elif "Datanet-EDX-Transform-" not in job_og_desc:
                job_new_desc=job_og_desc.replace(" ","-")
                job_prof_desc="Datanet-EDX-Transform-"+job_new_desc
                while len(job_prof_desc) >=100:
                    print("Current Profile Description: ",job_prof_desc)
                    job_desc=input("Profile Description exceeds 100 characters, Enter new profile description less than 100 chars : ")
                    job_prof_desc=job_desc
                job_profile['jobProfile']['description']=job_prof_desc
            else:
                print(f"\nNo need to update {job_id} Redshift Load Description")
                return
        datanet_client.update_job_profile(UpdateJobProfileRequest(job_profile=job_profile['jobProfile'],request_context=RequestContext(login_name="sweekyp")))
        print(
            f"\nSuccessfully updated profile_id:{job_id}"
        )
    except Exception as e:
        print(
            f"error occuried while processing profile_id:{job_id}. ErrorMessage: {e} "
        )
 
def main():
    print(os.getcwd())
    pipeline_path=input("Enter local build path of package: ")
    profile_count = int(input("Enter the number of profiles: "))
    print("Provide the profile type and id list [in a new line]: ")
    profiles = []
    session = sentry_init()
 
    for _ in range(profile_count):
        profile_type = input()
        profile_id = input()
        profiles.append((profile_type, profile_id))
 
    for profile_type, profile_id in profiles:
        if profile_type in ("INCREMENTAL","TRANSFORM","DATA_FEED","LOAD","SQL_LOAD","CONVERSION","METRICS","CONSOLIDATION","ANDES_LOAD"):
            update_profile(int(profile_id),profile_type)
        elif profile_type in ("CRADLE"):
            update_cradle_profile_desc(session, profile_id)
            
    os.chdir(pipeline_path)
    print(os.getcwd())
    with open("inputProfiles.csv", "w"):
        pass
 
    def_folder = None
    for root, dirs, files in os.walk(os.getcwd()):
        for dir_name in dirs:
            if "def" in dir_name and not "example" in dir_name:
                def_folder = os.path.join(root, dir_name)
                break
        if def_folder:
            break
    if not def_folder:
        raise Exception("Folder containing 'def' not found.")
 
    parts = def_folder.split("/")
    extracted = parts[-1]
    print(os.getcwd())
 
    for profile_type, profile_id in profiles:
        if profile_type in ("INCREMENTAL", "TRANSFORM", "DATA_FEED", "LOAD", "SQL_LOAD", "CONVERSION", "METRICS", "CONSOLIDATION", "ANDES_LOAD"):
            csv_content = f"{profile_type},{profile_id}\n"  
            with open("inputProfiles.csv", "a") as file:  
                file.write(csv_content)  
    
    run_cmd("toolbox update bdtmaestro && bdtmaestro export --input inputProfiles.csv -ue --platform datanet -et ALARM")
    for profile_type, profile_id in profiles:
        if profile_type in ("INCREMENTAL", "TRANSFORM", "DATA_FEED", "LOAD", "SQL_LOAD", "CONVERSION", "METRICS", "CONSOLIDATION", "ANDES_LOAD"):
            res1 = sentry_get(session, "https://datanet-service.amazon.com/jobProfile/" + profile_type + "/" + profile_id)
            if res1.status_code == 404 or res1.text == 'null' or res1.text == '':
                print("Datanet Profile not found")
                return
 
            profile = json.loads(res1.text)
            profile_description=profile['jobProfile']['description']
            enter_folder(extracted)
            enter_folder(profile_description)
            folder_path = os.getcwd()
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    if file_name.endswith(".json"):
                        file_path = os.path.join(root, file_name)
                        if "/jobs/" in file_path:
                            if "ANDES_LOAD" in profile_type:
                                update_andes_job(file_path)
                            elif "LOAD" in profile_type:
                                update_rs_job(file_path)
                            elif "TRANSFORM" in profile_type:
                                update_tranf_job(file_path)
 
                        else:
                            if "ANDES_LOAD" in profile_type:
                                update_andes_profile(file_path)
                            elif "LOAD" in profile_type:
                                update_rs_profile(file_path)
                            elif "TRANSFORM" in profile_type:
                                update_tranf_profile(file_path) 
            os.chdir(pipeline_path)    
 
        else:
            with open("inputProfileId.csv", "w") as file:
                file.write(f"{profile_id}")
 
            res1 = sentry_get(session, "https://dryadservice-na-iad.iad.proxy.amazon.com/profiles/" + profile_id)
            if res1.status_code == 404 or res1.text == 'null' or res1.text == '':
                print("Cradle Profile not found")
                return
 
            profile = json.loads(res1.text)
            profile_description=profile["profile"]["name"]
 
            crad_folder=None
            for root, dirs, files in os.walk(def_folder):
                for dir_name in dirs:
                    dname=dir_name.lower()
                    if "cradle" == dname:
                        crad_folder = os.path.join(root, dir_name)
                        break
                if crad_folder:
                    break
            if not crad_folder:
                raise Exception("Folder containing 'cradle' not found.")
                
            splitt = crad_folder.split("/")
            crad_ext = splitt[-2]+"/"+splitt[-1]
            run_cmd(f"toolbox update bdtmaestro && bdtmaestro export --input inputProfileId.csv -ud {crad_ext} --platform cradle")
 
            enter_folder(crad_ext)
            enter_folder(profile_description)
            folder_path = os.getcwd()
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    if file_name.endswith(".json"):
                        file_path = os.path.join(root, file_name)
                        if "jobs" in file_path:
                            update_cradle_job(file_path)
                        else:
                            update_cradle_profile(file_path)
            os.chdir(pipeline_path)  
 
    session.close()
 
if __name__ == "__main__":
    main()
