#!/bin/bash

# This script copies a table definition from the CustomerDNA beta provider to the prod provider.
# It also creates a new table version under the same table in the prod provider.
# When running the script, pass the table_name, test_table_name, beta_provider, and prod_provider as arguments.
# i.e. ./MaestroAndesTableCreation.sh DC_TEST_TABLE_SINPARUP DC_TEST_TABLE_SINPARUP_TEST_11 bic_ddl bic_ddl

# Validation
if ! [ ./validate-prerequisites ]; then
    echo "Validation failed."
    exit 1
fi

# Command-line arguments
TABLE_NAME=$1
TEST_TABLE_NAME=$2
BETA_PROVIDER=$3
PROD_PROVIDER=$4
ANDES_SERVICE='https://andes-service-iad.iad.proxy.amazon.com/v2'

TMP_REQUEST=$(mktemp)
TMP_RESPONSE=$(mktemp)

# Function to check if a table exists in the prod provider
check_table_exists() {
    local table_name=$1
    local provider=$2
    local response
    response=$(kcurl -k -s --header 'Accept: application/table+json' \
      "$ANDES_SERVICE/providers/$provider/tables/$table_name" 2>/dev/null)
      
    # Check for specific error messages or status codes indicating table existence
    if echo "$response" | grep -q "HTTP Status 404"; then
        return 1  # Table does not exist
    elif echo "$response" | grep -q "HTTP Status 200"; then
        return 0  # Table exists
    else
        echo "Unexpected response or error: $response"
        return 2  # Error in checking table existence
    fi
}

# Function to create a new table in prod provider
create_new_table() {
    # Check if the table already exists
    if check_table_exists "$TEST_TABLE_NAME" "$PROD_PROVIDER"; then
        echo "Table $TEST_TABLE_NAME already exists in prod provider. Exiting."
        exit 1
    fi

    # Fetch the table definition from the beta provider
    kcurl -k -s --header 'Accept: application/table+json' \
      "$ANDES_SERVICE/providers/$BETA_PROVIDER/tables/$TABLE_NAME" > $TMP_RESPONSE

    # Create the request payload for creating the table in the prod provider
    cat $TMP_RESPONSE | jq "{
        tableName: \"$TEST_TABLE_NAME\",
        description,
        appendix,
        audit,
        providerId: \"$PROD_PROVIDER\",
        definition: .definition
    }" > $TMP_REQUEST

    echo "Creating $TEST_TABLE_NAME in prod provider with below arguments"
    cat $TMP_REQUEST

    # Send POST request to create the table in the prod provider
    kcurl -k -s -X POST \
        --header "Content-Type: application/table+json; version=2.0" \
        --data @$TMP_REQUEST \
        "$ANDES_SERVICE/providers/$PROD_PROVIDER/tables"
}

# Function to create a new table version in prod provider
create_new_table_version() {
    # Fetch latest version from beta provider
    BETA_LATEST_VERSION=$(kcurl -k -s --silent \
      "$ANDES_SERVICE/providers/$BETA_PROVIDER/tables/$TABLE_NAME/versions" | jq '[.data[]] | max_by(.versionNumber)')

    # Fetch latest version from prod provider
    PROD_LATEST_VERSION=$(kcurl -k -s --silent \
      "$ANDES_SERVICE/providers/$PROD_PROVIDER/tables/$TEST_TABLE_NAME/versions" | jq '[.data[]] | max_by(.versionNumber)')

    # Extract version numbers
    BETA_LATEST_VERSION_NUMBER=$(echo "$BETA_LATEST_VERSION" | jq -r ".versionNumber")
    PROD_LATEST_VERSION_NUMBER=$(echo "$PROD_LATEST_VERSION" | jq -r ".versionNumber")

    # Set PROD_LATEST_VERSION_NUMBER to 1 if it is null (indicating no existing versions)
    if [[ "$PROD_LATEST_VERSION_NUMBER" == "null" ]]; then
        echo "Creating first version"
        PROD_LATEST_VERSION_NUMBER=1
    else
        PROD_LATEST_VERSION_NUMBER=$((PROD_LATEST_VERSION_NUMBER + 1))
    fi

    # Fetch SDL schema and DataplaneType from beta provider
    SCHEMA_RESPONSE=$(kcurl -k -s --silent \
      "$ANDES_SERVICE/providers/$BETA_PROVIDER/tables/$TABLE_NAME/versions/$BETA_LATEST_VERSION_NUMBER/schema")

    SDL_SCHEMA=$(echo "$SCHEMA_RESPONSE" | jq -r ".schema.sdl")
    DATAPLANE_TYPE=$(echo "$SCHEMA_RESPONSE" | jq -r ".dataplaneType")
    PRIMARY_KEYS=$(echo "$SCHEMA_RESPONSE" | jq ".schema.primaryKeys")

    # Handle case where DATAPLANE_TYPE is null
    if [ "$DATAPLANE_TYPE" == "null" ]; then
        echo "dataplaneType is null, please check the schema response."
        DATAPLANE_TYPE=""
    fi

    echo "Schema_sdl: $SDL_SCHEMA"
    echo "Dataplane Type: $DATAPLANE_TYPE"
    echo "Primary Keys: $PRIMARY_KEYS"

    # Construct new table version object for prod provider
    NEW_TABLE_VERSION=$(echo "$BETA_LATEST_VERSION" | jq --arg sdl "$SDL_SCHEMA" --arg dpt "$DATAPLANE_TYPE" --argjson pks "$PRIMARY_KEYS" '{
        providerId: "'"$PROD_PROVIDER"'",
        tableName: "'"$TEST_TABLE_NAME"'",
        versionNumber: '"$PROD_LATEST_VERSION_NUMBER"',
        definition: {
            contentType: .definition.contentType,
            partitionKeys: .definition.partitionKeys,
            schema: {
                primaryKeys: $pks,
                sdl: $sdl
            },
            partitionType: .definition.partitionType,
            dataplaneType: .definition.dataplaneType,
            dataplaneDetails: {
                cairns: {
                    region: .definition.dataplaneDetails.cairns.region
                }
            }
        },
        description: .description,
        lifecycleState: "UNRELEASED",
        appendix: .appendix,
        audit: .audit,
        isLakeFormationEnabled: .isLakeFormationEnabled
    }' | jq 'del(.definition.schema.sdlReference)' | jq 'del(.appendix.entries.cairnsStream)' | jq 'del(.appendix.entries."Completeness@1.0")')

    echo "Copying latest version for $TEST_TABLE_NAME with below arguments"
    echo "$NEW_TABLE_VERSION" > "$TMP_REQUEST"
    cat "$TMP_REQUEST"

    # Send POST request to create new table version in prod provider
    kcurl -k -s --silent -X POST \
        --header "Content-Type: application/table_version+json; version=1.7" \
        --data @"$TMP_REQUEST" \
        "$ANDES_SERVICE/providers/$PROD_PROVIDER/tables/$TEST_TABLE_NAME/versions"
}

# Create new table in prod provider
create_new_table

# Create new table version in prod provider
create_new_table_version

# Clean up temporary files
rm "$TMP_REQUEST" "$TMP_RESPONSE"
