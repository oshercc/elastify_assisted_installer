
REMOVED_FEALDS = [
    "cluster_image_info_ssh_public_key",
    "cluster_ssh_public_key,"
    ""
]

def process_hosts_count(j):
    host_jsons = j["cluster_hosts"]
    return len(host_jsons)

def process_hosts_synced_ntp_count(j):
    host_jsons = j["cluster_hosts"]
    return  len(["*" for host in host_jsons if "Host NTP is synced" in host['validations_info']])

def process_cluster_ntp_synced(j):
    return  j["process_hosts_count"] == j["process_hosts_synced_ntp_count"]

def process_fields(json_file):

    process = [process_hosts_count,
               process_hosts_synced_ntp_count,
               process_cluster_ntp_synced
               ]

    for process in process:
        try:
            json_file[process.__name__] = process(json_file)
        except:
            continue

def remove_customer_access_fields(json_file):
    for remove_field in REMOVED_FEALDS:
        json_file.pop(remove_field, None)

def process_metadata(json_file):

    remove_customer_access_fields(json_file)
    process_fields(json_file)

    return json_file