from aaa_client import AAAClient
from com.amazon.tablesubscriptionservice.tablesubscriptionservice import TableSubscriptionServiceClient
from com.amazon.tablesubscriptionservice.createsubscriptionv3request import CreateSubscriptionV3Request
from com.amazon.tablesubscriptionservice.sourcedetails import SourceDetails
from coral_aaa.rpc import new_orchestrator
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG)

endpoint = 'https://table-subscription-service-iad.iad.proxy.amazon.com/'
description = "Testing DC Utility"
version_migration_strategy = "VERSIONED"

def create_subscription(subscription_target_id, provider_id, table_name, table_version, timeout=60):
    try:
        print("Initializing orchestrator and client")
        # Initialize orchestrator and client
        orchestrator = new_orchestrator(
            timeout=timeout,
            endpoint=endpoint,
            version=1,
            aaa_client=AAAClient()
        )
        client = TableSubscriptionServiceClient(orchestrator=orchestrator)

        print("Preparing source details and request")
        # Prepare source details and request
        source_details = SourceDetails(
            provider_id=provider_id,
            table_name=table_name,
            table_version=table_version
        )
        request = CreateSubscriptionV3Request(
            description=description,
            subscription_target_id=subscription_target_id,
            source_details=source_details,
            version_migration_strategy=version_migration_strategy
        )

        print("Creating subscription")
        # Create subscription and return response
        response = client.create_subscription_v3(request)
        print("Subscription created successfully!!")
        return response
    except Exception as e:
        print("An error occurred during subscription creation")
        print(str(e))
        return str(e)

# Example usage
def main():
    print("Calling create_subscription")
    response = create_subscription(subscription_target_id, 'bic_ddl', table_name, table_version)
    print("Response received")
    print("Response:", response)

if __name__ == "__main__":
    #subscription_target_id = '66c373b5-7da5-7932-b995-da13943843ee'
    #table_name = 'O_NON_DC_PEND_CUST_SHIPMENTS_MAESTRO'
    #table_version = '6'
    print("Running main")
    main()
