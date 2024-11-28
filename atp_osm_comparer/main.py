import AtpDataManager
import OSMDataManager
import osm_replicator
import WebManager
import time

data_path = "./data/"

atp_data_manager = AtpDataManager.ATPDataManager(data_path)
new_atp_data = atp_data_manager.download_atp()
atp_data_manager.manage_pickles(False)

osm_data_manager = OSMDataManager.OSMDataManager(data_path, atp_data_manager.set_elements)

web_manager = WebManager.WebManager(data_path, atp_data_manager.set_elements, osm_data_manager.node_list, osm_data_manager.way_list, osm_data_manager.relation_list)

osm_replicator = osm_replicator.OsmReplicator(atp_data_manager.set_elements, osm_data_manager.osm_timestamp, data_path, osm_data_manager.node_list, osm_data_manager.way_list, osm_data_manager.relation_list, web_manager)

last_download_time = None

while True:
    if last_download_time is None or time.time() - last_download_time > 3600:

        if atp_data_manager.download_atp():
            osm_data_manager.node_list.delete_pickle_file()
            osm_data_manager.way_list.delete_pickle_file()
            osm_data_manager.relation_list.delete_pickle_file()
        if osm_data_manager.download_osm():
            web_manager.update_json()
        last_download_time = time.time()

    processed_seqid = osm_replicator.start_replicator()
    if processed_seqid == None:
        time.sleep(10)
