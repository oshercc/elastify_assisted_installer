import os
import json
import glob
import process
import logging
import hashlib
import argparse
import subprocess
import collections
import elasticsearch

logging.basicConfig(level=logging.WARN, format='%(levelname)-10s %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("__main__").setLevel(logging.INFO)

MARKED_DIR_PREFIX = "MARK"
VERSION = 11
INDEX = "ai_events"
MARKED_DIR = "{}_{}".format(MARKED_DIR_PREFIX, VERSION)

def main(data_path, elastic_server, index, dry_run=False):
    logger.info("Starting log collection to db")
    cluster_log_file = get_files(data_path)
    if not dry_run:
        es = elasticsearch.Elasticsearch([elastic_server])

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

            if not dry_run:
                res = es.index(index=index, body=cluster_metadata_json, id=id_)
                logger.info("index {}, result {}".format(INDEX, res['result']))
            mark_dir(cluster_log_path)

def generate_id(event_json):
    id_str = event_json["event_time"] + event_json["cluster_id"] + event_json["message"]
    _id = int(hashlib.md5(id_str.encode('utf-8')).hexdigest(), 16)
    return str(_id)

def process_metadata(cluster_metadata_json):
    return process.process_metadata(cluster_metadata_json)

def process_events(cluster_events_json):
    return  process.process_events(cluster_events_json)

def flatten_metadata(cluster_metadata_json):
    return flatten(cluster_metadata_json)

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

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

def get_files(data_path):
    cluster_dirs = list()
    for cluster_log_dir in glob.glob(data_path + '/*'):
        if log_dir_is_marked(cluster_log_dir):
            continue
        cluster_dirs.append(cluster_log_dir)
    logger.info("Collected files {}".format(len(cluster_dirs)))
    return cluster_dirs

def log_dir_is_marked(path):
    if MARKED_DIR in [os.path.basename(x) for x in glob.glob(path + '/*')]:
        return True
    return False

def mark_dir(path):
    for mark_path in glob.glob(os.path.join(path,MARKED_DIR_PREFIX + "*")):
        subprocess.check_output("rm -f {}".format(mark_path), shell=True)
    subprocess.check_output("touch {}".format(os.path.join(path,MARKED_DIR)), shell=True)
    logger.info("marked path {}".format(path))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-elastic-server", help="elastic db address", default="")
    parser.add_argument("-dp", "--data-path", help="Path to log directories")
    parser.add_argument("--dry-run", help="Test run", action='store_true')
    parser.add_argument("-index", help="es index", default=INDEX)
    args = parser.parse_args()

    main(data_path = args.data_path, elastic_server = args.elastic_server, dry_run=args.dry_run, index=args.index )
