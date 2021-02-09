#! /usr/bin/env python3

from log_files_to_elastic import *
import argparse
import json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVER = 'kafka.datahub.redhat.com:443'
DATA_HUB_KAFKA_CA = 'kafka/data-hub-kafka-ca.crt'

MARKED_DIR_PREFIX = "MARK_PUBLIC"
VERSION = 1
INDEX = "dynamic-assisted-service-events"
MARKED_DIR = "{}_{}".format(MARKED_DIR_PREFIX, VERSION)
DATA_PATH = "/var/ai-logs"

logging.basicConfig(level=logging.WARN, format='%(levelname)-10s %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("__main__").setLevel(logging.INFO)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bootstrap', default=KAFKA_BOOTSTRAP_SERVER)
    parser.add_argument('-c', '--cacert', default=DATA_HUB_KAFKA_CA)
    parser.add_argument('-t', '--topic', required=True)
    parser.add_argument('-p', '--payload')
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             security_protocol='SSL',
                             ssl_cafile=args.cacert,
                             api_version_auto_timeout_ms=30000,
                             max_block_ms=900000,
                             request_timeout_ms=450000,
                             acks='all')

    while True:
        logger.info("Starting log collection to db")
        cluster_log_file = get_files(DATA_PATH)
        # if not dry_run:
        #     es = elasticsearch.Elasticsearch([elastic_server])

        for cluster_log_path in cluster_log_file:

            cluster_events_json = get_cluster_events_json(cluster_log_path)
            events_extract = process_events(cluster_events_json)

            cluster_metadata_json = get_cluster_metadata_json(cluster_log_path)
            cluster_metadata_json = flatten_metadata(cluster_metadata_json)

            cluster_metadata_json = process_metadata(cluster_metadata_json)

            cluster_metadata_json.update(events_extract)

            for event in cluster_events_json:
                cluster_metadata_json.update(event)

                logger.info("add {} to db".format(event))
                id_ = generate_id(cluster_metadata_json)
                cluster_metadata_json.update(id_)

                producer.send(INDEX, cluster_metadata_json.encode('utf-8'))
                producer.flush()  # Important, especially if message size is small

                mark_dir(cluster_log_path)


if __name__ == '__main__':
    main()
