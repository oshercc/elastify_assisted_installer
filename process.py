
def process_metadata(json_file):

    if "cluster_hosts" in json_file:
        host_jsons = json_file["cluster_hosts"]

        json_file["hosts_count"]            =  len(host_jsons)
        json_file["hosts_synced_ntp_count"] = len(["*" for host in host_jsons if  "Host NTP is synced" in host['validations_info']])
        json_file["all_hosts_ntp_synced"]   = json_file["hosts_count"] == json_file["hosts_synced_ntp_count"]

    return json_file

