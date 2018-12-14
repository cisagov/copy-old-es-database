import logging
import time

import boto3
import requests
from requests_aws4auth import AWS4Auth

OLD_AWS_PROFILE_NAME = 'old_account'
ES_REGION = 'us-east-1'
OLD_ES_URL = 'https://search-dmarc-import-elasticsearch-7ommkg6qt7a3c5bersj6a6ebaq.us-east-1.es.amazonaws.com/dmarc_aggregate_reports'
NEW_ES_URL = 'https://search-dmarc-import-elasticsearch-dtbgkfx23yppmjmothuy6t7wd4.us-east-1.es.amazonaws.com/dmarc_aggregate_reports'
ES_RETRIEVE_SIZE = 10000
SLEEP_BETWEEN_RETRIEVALS = 2


def process_hits(hits, old_awsauth):
    new_aws_credentials = boto3.Session().get_credentials()
    new_awsauth = AWS4Auth(new_aws_credentials.access_key,
                           new_aws_credentials.secret_key,
                           ES_REGION, 'es',
                           session_token=new_aws_credentials.token)
    full_url = '{}/_doc'.format(NEW_ES_URL)

    for hit in hits:
        success = True
        try:
            response = requests.post(full_url,
                                     auth=new_awsauth,
                                     json=hit['_source'],
                                     headers={'Content-Type': 'application/json'},
                                     timeout=300)
            # Raises an exception if we didn't get back a 200 code
            response.raise_for_status()
        except requests.exceptions.RequestException:
            logging.exception('Unable to save the DMARC aggregate report to the new Elasticsearch')
            success = False

        if success:
            # Delete the hit from the old database
            try:
                response = requests.delete('{}/report/{}'.format(OLD_ES_URL, hit['_id']),
                                           auth=old_awsauth,
                                           timeout=300)
                # Raises an exception if we didn't get back a 200 code
                response.raise_for_status()
            except requests.exceptions.RequestException:
                logging.exception('Unable to delete the DMARC aggregate report with ID {} from the old Elasticsearch'.format(hit['id']))


def main():
    # Set up logging
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(message)s',
                        level=logging.INFO)

    # Construct the auth from the old AWS credentials
    old_aws_credentials = boto3.Session(profile_name=OLD_AWS_PROFILE_NAME).get_credentials()
    old_awsauth = AWS4Auth(old_aws_credentials.access_key,
                           old_aws_credentials.secret_key,
                           ES_REGION, 'es',
                           session_token=old_aws_credentials.token)

    # Now construct the query.  We want all DMARC aggregate reports
    # ordered by increasing age.
    query = {
        'size': ES_RETRIEVE_SIZE,
        'sort': [
            {
                'report_metadata.date_range.begin': {
                    'order': 'asc'
                }
            }
        ]
    }

    query_again = True
    while query_again:
        # Now perform the query
        response = requests.get('{}/_search'.format(OLD_ES_URL),
                                auth=old_awsauth,
                                json=query,
                                headers={'Content-Type': 'application/json'},
                                timeout=300)
        # Raises an exception if we didn't get back a 200 code
        response.raise_for_status()

        hits = response.json()['hits']['hits']

        # process hits
        logging.info('Got {} hits'.format(len(hits)))
        process_hits(hits, old_awsauth)

        # If there were fewer hits than ES_RETRIEVE_SIZE then there is no
        # need to keep querying
        if len(hits) < ES_RETRIEVE_SIZE:
            query_again = False

        # Sleep to give the just-deleted documents a chance to be
        # properly removed by the old database
        time.sleep(SLEEP_BETWEEN_RETRIEVALS)


if __name__ == '__main__':
    main()
