import os
import json
import glob
import uuid
import logging
import argparse
import subprocess
import collections
import elasticsearch

logging.basicConfig(level=logging.WARN, format='%(levelname)-10s %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("__main__").setLevel(logging.INFO)

MARKED_DIR = "MARK_1"
INDEX = "ai_events"

def main(data_path, elastic_server):
    logging.info("Starting log collection to db")
    cluster_log_file = get_files(data_path)
    es = elasticsearch.Elasticsearch([elastic_server])

    for cluster_log_path in cluster_log_file:
        cluster_events_json = get_cluster_events_json(cluster_log_path)
        cluster_metadata_json = get_cluster_metadata_json(cluster_log_path)
        cluster_metadata_json = flatten(cluster_metadata_json)

        for event in cluster_events_json:
            cluster_metadata_json.update(event)

            logging.info("add {} to db".format(event))
            res = es.create(index=INDEX, body=cluster_metadata_json, id=str(uuid.uuid1()))
            logging.info("index {}, result {}".format(str(uuid.uuid1()), res['result']))
            mark_dir(cluster_log_path)

def get_cluster_events_json(path):
    path_template = "cluster_*_events.json"
    event_file = glob.glob(os.path.join(path, path_template))[0]
    with open(event_file) as json_file:
        data = json.load(json_file)
    return data

def get_cluster_metadata_json(path):
    event_file_path = os.path.join(path, "metdata.json")
    with open(event_file_path) as json_file:
        data = json.load(json_file)
    return data


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_files(data_path):
    cluster_dirs = list()
    for cluster_log_dir in glob.glob(data_path + '/*'):
        if log_dir_is_marked(cluster_log_dir):
            continue
        cluster_dirs.append(cluster_log_dir)
    logging.info("Collected files {}".format(len(cluster_dirs)))
    return cluster_dirs

def log_dir_is_marked(path):
    if MARKED_DIR in [os.path.basename(x) for x in glob.glob(path + '/*')]:
        return True
    return False

def mark_dir(path):
    subprocess.check_output("touch {}".format(os.path.join(path,MARKED_DIR)), shell=True)
    logging.info("marked path {}".format(path))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-elastic-server", help="elastic db address", default="")
    parser.add_argument("-dp", "--data-path", help="Path to log directories")
    args = parser.parse_args()

    main(data_path = args.data_path, elastic_server = args.elastic_server)
