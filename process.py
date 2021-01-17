
REMOVED_FIELDS = [
    "cluster_image_info_ssh_public_key",
    "cluster_ssh_public_key"
]

def process_hosts_count(j):
    host_jsons = j["cluster_hosts"]
    return len(host_jsons)

def process_hosts_synced_ntp_count(j):
    host_jsons = j["cluster_hosts"]
    return len(["*" for host in host_jsons if "Host NTP is synced" in host['validations_info']])

def process_cluster_ntp_synced(j):
    return j["process_hosts_count"] == j["process_hosts_synced_ntp_count"]

def process_fields(cluster_metadata_json):

    process = [process_hosts_count,
               process_hosts_synced_ntp_count,
               process_cluster_ntp_synced
               ]

    for process in process:
        try:
            cluster_metadata_json[process.__name__] = process(cluster_metadata_json)
        except:
            continue

def remove_customer_access_fields(j):
    for remove_field in REMOVED_FIELDS:
        j.pop(remove_field, None)

def process_metadata(cluster_metadata_json):

    remove_customer_access_fields(cluster_metadata_json)
    process_fields(cluster_metadata_json)

    return cluster_metadata_json