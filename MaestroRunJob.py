from com.amazon.datanet.service.runjobrequest import RunJobRequest
from com.amazon.datanet.service.getjobrunstatusrequest import GetJobRunStatusRequest
from bdt_content_py_utils.datanetUtilFiles.datanetClient import get_datanet_client
from com.amazon.datanet.model.datasettimeintervaldate import DatasetTimeIntervalDate
import datetime
import time

def run_job(client, job_id, start_date, end_date):
    try:
        dataset_interval = DatasetTimeIntervalDate(
            dataset_date=str(start_date),
            dataset_time_interval_start=str(start_date) + 'T00:00:00Z',
            dataset_time_interval_end=str(end_date) + 'T00:00:00Z'
        )
        print(f"dataset_interval -> {dataset_interval}")

        run_job_response = client.run_job(RunJobRequest(job_id=int(job_id), interval=dataset_interval))
        print(f"Run job response for job_id {job_id}: {run_job_response}")

        time.sleep(10)
        
        job_run_id = run_job_response.job_runs[0].id
        job_status_response = client.get_job_run_status(GetJobRunStatusRequest(job_run_id=int(job_run_id)))
        current_status = job_status_response.job_run_status
        print(f"Current job status for job_id {job_id}: {current_status}")
        
        if current_status == 'ERROR':
            raise IOError(f'Job {job_id} Ran into ERROR')

    except Exception as exp:
        print(f'Error during job execution for job_id {job_id}: {exp}')
        raise IOError(f'Running Job {job_id} Failed with reason: {exp}')

def run_jobs_for_ids(datanet_client, job_ids, start_date, end_date):
    for job_id in job_ids:
        run_job(datanet_client, job_id, start_date, end_date)
