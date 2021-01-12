

def process_metadata(json_file):
    Process = [process_hosts_count,
               process_hosts_synced_ntp_count,
               process_cluster_ntp_synced
               ]

    for process in Process:
        try:
            json_file = process(json_file)
        except:
            continue

    if "cluster_hosts" in json_file:
        host_jsons = json_file["cluster_hosts"]

        json_file["process_hosts_count"]            = len(host_jsons)
        json_file["process_hosts_synced_ntp_count"] = len(["*" for host in host_jsons if  "Host NTP is synced" in host['validations_info']])
        json_file["process_cluster_ntp_synced"]     = json_file["process_hosts_count"] == json_file["process_hosts_synced_ntp_count"]

    return json_file

def process_hosts_count(j):
    host_jsons = j["cluster_hosts"]
    return len(host_jsons)

def process_hosts_synced_ntp_count(j):
    host_jsons = j["cluster_hosts"]
    return  len(["*" for host in host_jsons if "Host NTP is synced" in host['validations_info']])

def process_cluster_ntp_synced(j):
    return  j["process_hosts_count"] == j["process_hosts_synced_ntp_count"]

