import AtpDataManager
import OSMDataManager
import osm_replicator
import WebManager


data_path = "./data/"

atp_data_manager = AtpDataManager.ATPDataManager(data_path)

new_atp_downloaded = atp_data_manager.download_atp()

osm_data_manager = OSMDataManager.OSMDataManager(data_path, atp_data_manager.set_elements)

osm_data_manager.download_osm(new_atp_downloaded)

web_manager = WebManager.WebManager(data_path, atp_data_manager.set_elements, osm_data_manager.node_list, osm_data_manager.way_list, osm_data_manager.relation_list)

osm_replicator.start_replicator(atp_data_manager.set_elements, osm_data_manager.osm_timestamp, data_path, osm_data_manager.node_list, osm_data_manager.way_list, osm_data_manager.relation_list)