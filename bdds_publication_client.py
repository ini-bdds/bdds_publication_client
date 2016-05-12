#!/usr/bin/env python

import argparse
import globus_sdk
import requests
import json
import pprint
import time
import sys
from transfer_bindings import TransferBindingsClient, TransferBaseEntity

service_url = "https://publish.globus.org/v1/api/"

auth_token = None
headers = None
printer = pprint.PrettyPrinter(indent=4)


def load_file(file):
    with open(file, 'r') as f:
        data = f.read()
    data = data.strip()
    return data


def load_metadata(file):
    rVal = json.loads(load_file(file))
    return rVal


def prompt_for_metadata():
    return (27, {})


def get_headers():
    if auth_token:
        return {'Authorization': 'Bearer ' + auth_token}
    else:
        return None


def post_request(path, json_body=None):
    path = service_url + path
    headers = get_headers()
    return requests.post(path, headers=headers, json=json_body)


def delete_request(path, json_body=None):
    path = service_url + path
    headers = get_headers()
    return requests.delete(path, headers=headers, json=json_body)


def post_json(path, json_body=None):
    r = post_request(path, json_body=json_body)
    if r.status_code < 400:
        return r.json()
    else:
        r.raise_for_status()


def delete_json(path, json_body=None):
    r = delete_request(path, json_body=json_body)
    if r.status_code < 400:
        return r.json
    else:
        r.raise_for_status()


def get_request(path):
    path = service_url + path
    headers = get_headers()
    return requests.get(path, headers=headers)


def get_json(path):
    r = get_request(path)
    if r.status_code < 400:
        return r.json()
    else:
        r.raise_for_status()


def print_schemas():
    schemas = list_schemas()
    for schema in schemas:
        print_schema(schema)


def print_schema(schema):
    printer.pprint(schema)

def print_dataset(dataset):
    printer.pprint(dataset)

def list_schemas():
    return get_json('schemas')


def display_schema(target_schema_id):
    r = get_json('schemas/' + target_schema_id)
    print_schema(r)


def print_collections():
    colls = list_collections()
    for coll in colls:
        print_collection(coll)


def print_collection(coll):
    printer.pprint(coll)


def list_collections():
    return get_json('collections')


def push_metadata(collection, metadata):
    path = "collections/" + str(collection)
    return post_json(path, json_body=metadata)


def get_dataset(dataset_id):
    path = "datasets/" + dataset_id
    return get_json(path)

def get_dataset(dataset_pid):
    path = "datasets?uri=" + dataset_pid
    return get_json(path)

def delete_dataset(dataset_id):
    path = "datasets/" + dataset_id
    return delete_json(path)

def complete_submission(submission_id):
    path = "/datasets/" + submission_id + "/submit"
    return post_json(path)


def perform_transfer(transfer_client, src_endpoint, src_path,
                     dest_endpoint, dest_path):
    sub_id = transfer_client.create_submissionid().value
    print 'submission id: ', sub_id
    transfer_item = TransferBaseEntity(DATA_TYPE='transfer_item',
                                       source_path=src_path,
                                       destination_path=dest_path,
                                       recursive=True)
    transfer_list = [transfer_item]
    transfer = TransferBaseEntity(DATA_TYPE='transfer',
                                  submission_id=sub_id,
                                  notify_on_succeeded=False,
                                  notify_on_failed=False,
                                  notify_on_inactive=False,
                                  source_endpoint=src_endpoint,
                                  destination_endpoint=dest_endpoint,
                                  DATA=transfer_list
                                  )
    transfer_json = transfer.tojson()
    print 'Transfer Job JSON: ', transfer_json
    r = transfer_client.post('transfer', text_body=transfer_json)
    return TransferBaseEntity(globus_response=r)


def wait_for_transfer(transfer_client, transfer_id, poll_time=None):
    max_polls = -1
    if poll_time is not None:
        max_polls = int(poll_time) / 5
    poll_count = 0

    while max_polls < 0 or poll_count < max_polls:
        r = transfer_client.get('/task/' + transfer_id)
        status = TransferBaseEntity(globus_response=r)
        print 'Current status:', status.tojson()
        if status.status != 'ACTIVE':
            return status
        time.sleep(5)
    return status


def main():
    parser = argparse.ArgumentParser(description='Data Publication Client')
# Client configurations
    parser.add_argument('--service-url', dest='service_url',
                        help='URL of the service to invoke')
# Operation configurations
    parser.add_argument('--data-endpoint', dest='data_endpoint',
                        help='Endpoint containing data to be placed in the publication')
    parser.add_argument('--data-directory', dest='data_directory',
                        help='Directory within the endpoint containing data to be placed in the publication')
    parser.add_argument('--metadata-file', dest='metadata_file',
                        help='File containing metadata to be placed in the publication')
    parser.add_argument('--collection-id', dest='collection_id',
                        help='List available collections')
    parser.add_argument('--dataset-id', dest='dataset_id',
                        help='Id of dataset used for other actions')
    parser.add_argument('--dataset-pid', dest='dataset_pid',
                        help='Persistent identifier of dataset used for other actions')
    parser.add_argument('--transfer-id', dest='transfer_id',
                        help='Id of transfer used for other actions')
    parser.add_argument('--interactive', dest='interactive',
                        action='store_true',
                        help='User interactive mode to prompt for metadata values')

# Actions
    parser.add_argument('--list-schemas', dest='list_schemas',
                        action='store_true',
                        help='List all schemas present in the publication service')
    parser.add_argument('--list-collections', dest='list_collections',
                        action='store_true',
                        help='List available collections')
    parser.add_argument('--introspect-schema', dest='target_schema',
                        help='The id of a schema to display all the fields in')
    parser.add_argument('--create-dataset', dest='perform_create',
                        action='store_true',
                        help='Create a new dataset in a collection. --metadata-file and --collection-id must be provided')
    parser.add_argument('--get-dataset', dest='perform_get',
                        action='store_true',
                        help='Get a dataset from a collection.')
    parser.add_argument('--transfer-data', dest='perform_transfer',
                        action='store_true',
                        help='Transfer data into dataset storage. --data-endpoint and --data-directory must be provided.')
    parser.add_argument('--download-dataset', dest='perform_download',
                        action='store_true',
                        help='Download dataset into dataset storage. --data-endpoint and --data-directory must be provided.')
    parser.add_argument('--wait', dest='wait_for_transfer',
                        action='store_true',
                        help='Wait for transfers to complete before finishing submission and exiting. --submit must be performed or or --transfer-id must be provided.')
    parser.add_argument('--poll', dest='poll_time',
                        help='Number of seconds to poll for completion of transfers. Only one of --wait and --poll should be specified. Requirements are the same as --wait.')
    parser.add_argument('--submit', dest='perform_submit', action='store_true',
                        help='Submit dataset in to collection. Must be combined with --create-dataset or --dataset-id must be provided. Transfers must be complete as insured by --wait.')
    parser.add_argument('--delete-dataset', dest='perform_delete', action='store_true',
                        help='Delete a dataset. Must be combined with --create-dataset or --dataset-id must be provided (note that using with --create-dataset is non-sensical since the created dataset will immediately be deleted). The dataset must not have been submitted to its collection.')
    args = parser.parse_args()

#    print 'args: ', args

# Setup clients
    transfer_id = None

    auth_token = globus_sdk.config.get_auth_token(None)
    transfer_client = TransferBindingsClient()

    if args.service_url is not None:
        service_url = args.service_url

# Setup provided identifiers
    metadata = None
    src_endpoint = None
    src_path = None
    dest_endpoint = None
    dest_path = None
    metadata_file = None
    collection_id = None
    dataset_id = None
    transfer_id = None
    dataset_pid = None

# Setup user input values
    if args.data_endpoint is not None:
        result = transfer_client.endpoint_search(fulltext_filter=args.data_endpoint, offset=0,
                                                 limit=1, fields='id,activated')
        src_endpoint = result.DATA[0].id
        activated = result.DATA[0].activated
        if activated is None or not activated:
            result = transfer_client.autoactivate_endpoint(src_endpoint)

    if args.data_directory is not None:
        src_path = args.data_directory
    if args.metadata_file is not None:
        metadata_file = args.metadata_file
    if args.collection_id is not None:
        collection_id = args.collection_id
    if args.dataset_id is not None:
        dataset_id = args.dataset_id
    if args.transfer_id is not None:
        transfer_id = args.transfer_id
    if args.dataset_pid is not None:
            dataset_pid = args.dataset_pid
# Perform actions
    if args.list_schemas:
        print_schemas()

    if args.list_collections:
        print_collections()

    if args.target_schema is not None:
        display_schema(args.target_schema)

    if args.metadata_file is not None:
        metadata = load_metadata(args.metadata_file)
    if args.interactive:
        args.collection_id, metadata = prompt_for_metadata()

    if args.perform_create:
        if metadata is None or collection_id is None:
            print '--collection-id and --metadata-file must be provided with --create-dataset'
            sys.exit(1)
        else:
            result = push_metadata(collection_id, metadata)
            dest_endpoint = result['globus.shared_endpoint.name']
            dest_path = result['globus.shared_endpoint.path']
            dataset_id = result['id']
            print 'Dataset record created: '
            printer.pprint(result)

    if args.perform_transfer:
        if src_endpoint is None or src_path is None:
            print '--data-endpoint and --data-directory must be provided to perform transfers.'
            sys.exit(1)
        elif dest_endpoint is None and dataset_id is not None:
            dataset = get_dataset(dataset_id)
            dest_endpoint = dataset['globus.shared_endpoint.name']
            dest_path = dataset['globus.shared_endpoint.path']
            print 'Dataset record returned: '
            printer.pprint(dataset)
            dataset_id_returned = dataset['id']
#            assert dataset_id_returned == dataset_id, "Unexpected dataset id returned"
        if dest_endpoint is None or dest_path is None:
            print '--transfer-data must be combined with --create-dataset or an existing dataset_id must be provided.'
            sys.exit(1)
        transfer_id = perform_transfer(transfer_client, src_endpoint, src_path, dest_endpoint, dest_path + "data/")
        transfer_id = transfer_id.task_id
        print 'id of transfer task is ', transfer_id

    if args.perform_download:
        if src_endpoint is None or src_path is None:
            print '--data-endpoint and --data-directory must be provided to perform transfers.'
            sys.exit(1)
        elif dataset_pid is None:
            print '--dataset-pid must be provided'
            sys.exit(1)

        dataset = get_dataset(dataset_pid)
        dataset_endpoint = dataset['globus.shared_endpoint.name']
        dataset_path = dataset['globus.shared_endpoint.path']
        transfer_id = perform_transfer(transfer_client, dataset_endpoint, dataset_path, src_endpoint, src_path)
        transfer_id = transfer_id.task_id
        print 'id of transfer task is ', transfer_id


    if args.wait_for_transfer or args.poll_time:
        if args.wait_for_transfer and args.poll_time:
            print 'Only one of --wait and --poll should be specified'
            sys.exit(1)
        if transfer_id is None:
            print '--transfer-data or --transfer-id must be specified'
            sys.exit(1)
        wait_for_transfer(transfer_client, transfer_id, args.poll_time)

    if args.perform_submit:
        if dataset_id is None:
            print '--submit requires either --create-dataset or --dataset-id to be specified'
            sys.exit(1)
        complete_submission(dataset_id)

    if args.perform_delete:
        if dataset_id is None:
            print '--submit requires either --create-dataset or --dataset-id to be specified'
            sys.exit(1)
        delete_dataset(dataset_id)

    if args.perform_get:
        if dataset_pid is None:
            print '--get-dataset requires --dataset-pid to be specified'
            sys.exit(1)
        print_dataset(get_dataset(dataset_pid))


if __name__ == '__main__':
    main()
