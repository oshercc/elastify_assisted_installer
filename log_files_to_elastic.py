import os
import json
import glob
import uuid
import logging
import argparse
import subprocess
import elasticsearch

logging.basicConfig(level=logging.WARN, format='%(levelname)-10s %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("__main__").setLevel(logging.INFO)

MARKED_DIR_PREFIX = "MARK"
VERSION = 2
INDEX = "ai_events"
MARKED_DIR = "{}_{}".format(MARKED_DIR_PREFIX, VERSION)

def main(data_path, elastic_server, dry_run=False):
    logger.info("Starting log collection to db")
    cluster_log_file = get_files(data_path)
    if not dry_run:
        es = elasticsearch.Elasticsearch([elastic_server])

    for cluster_log_path in cluster_log_file:
        cluster_events_json = get_cluster_events_json(cluster_log_path)
        cluster_metadata_json = get_cluster_metadata_json(cluster_log_path)
        cluster_metadata_json = flatten_metadata(cluster_metadata_json)

        for event in cluster_events_json:
            cluster_metadata_json.update(event)

            logger.info("add {} to db".format(event))
            if not dry_run:
                res = es.create(index=INDEX, body=cluster_metadata_json, id=str(uuid.uuid1()))
                logger.info("index {}, result {}".format(str(uuid.uuid1()), res['result']))
            mark_dir(cluster_log_path)


def flatten_metadata(cluster_metadata_json):
    cluster_metadata_json = flatten_json(cluster_metadata_json)
    for key, val in cluster_metadata_json.items():
        if isinstance(val, str) and is_json(val):
            cluster_metadata_json[key] = json.loads(val)
    return cluster_metadata_json


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


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

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
    args = parser.parse_args()

    main(data_path = args.data_path, elastic_server = args.elastic_server, dry_run=args.dry_run)
