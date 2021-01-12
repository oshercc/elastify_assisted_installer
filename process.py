

def process_metadata(json_file):
    Process = [process_hosts_count,
               process_hosts_synced_ntp_count,
               process_cluster_ntp_synced
               ]

    for process in Process:
        try:
            json_file[process.__name__] = process(json_file)
        except:
            continue
    return json_file

def process_hosts_count(j):
    host_jsons = j["cluster_hosts"]
    return len(host_jsons)

def process_hosts_synced_ntp_count(j):
    host_jsons = j["cluster_hosts"]
    return  len(["*" for host in host_jsons if "Host NTP is synced" in host['validations_info']])

def process_cluster_ntp_synced(j):
    return  j["process_hosts_count"] == j["process_hosts_synced_ntp_count"]

