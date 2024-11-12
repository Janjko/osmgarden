import requests
import osmium as o
import os
from collections import namedtuple
import xml.etree.ElementTree as ET
import libtorrent as lt
import time
from autosavearray import AutoSaveArray
import osm_matching_to_atp as omta

OSM_Set = namedtuple('OSM_Set', ['name', 'element'])

class OSMDataManager():
    def __init__(self, data_folder, atp_sets):
        self.osm_history_url = "https://planet.openstreetmap.org/pbf/planet-pbf-rss.xml"
        self.data_folder = data_folder
        self.osm_planet_filename_prefix = "planet-"
        self.osm_filtered_filename_prefix = "filtered_planet-"
        self.osm_nodes_set_prefix = "pickled_nodes"
        self.osm_ways_set_prefix = "pickled_ways"
        self.osm_relations_set_prefix = "pickled_relations"
        self.osm_pickeled_set_extension = ".bin"
        self.osm_extension = ".osm.pbf"
        self.osm_data_file, self.current_osm_run_id = self.get_file_by_prefix(self.data_folder, self.osm_filtered_filename_prefix, self.osm_extension)
        self.osm_data_filtered = True
        if self.osm_data_file == None:
            self.osm_data_filtered = False
            self.osm_data_file, self.current_osm_run_id = self.get_file_by_prefix(self.data_folder, self.osm_planet_filename_prefix, self.osm_extension)
        self.node_list = AutoSaveArray(os.path.join(self.data_folder, self.osm_nodes_set_prefix+self.osm_pickeled_set_extension))
        self.way_list = AutoSaveArray(os.path.join(self.data_folder, self.osm_ways_set_prefix+self.osm_pickeled_set_extension))
        self.relation_list = AutoSaveArray(os.path.join(self.data_folder, self.osm_relations_set_prefix+self.osm_pickeled_set_extension))
        self.osm_timestamp = None

        self.atp_sets = atp_sets


    def get_file_by_prefix(self, directory, prefix, extension):
        for file_name in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, file_name)) and file_name.startswith(prefix):
                # Get the rest of the name after the prefix
                rest_of_name = file_name[len(prefix):].strip(extension)
                return os.path.join(directory, file_name), rest_of_name
        return None, None

    def download_file(self, url, data_path):
        local_filename = data_path + url.split('/')[-1]
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename
    
    def download_osm(self, force):
        """Download OSM extract.

        Data is downloaded into data_path, as planet-latest.osm.pbf.

        Stores date of last downloaded data in the runs.json file in data_path.
        
        Returns path only if newer set is downloaded. If not, returns None.
        """

        response = requests.get(self.osm_history_url)
        
        if response.status_code == 200:
            rss_xml = response.content

            root = ET.fromstring(rss_xml)

            item = root.find(".//item")
            guid = item.find('guid').text
            link = item.find('link').text
            if 'planet-' in guid and '.osm.pbf' in guid:
                # Extract the date portion from the guid
                date_str = guid.split('planet-')[1].split('.osm.pbf')[0]

                if date_str != self.current_osm_run_id:
                    # Add the new run_id to the local array
                    torrent_file = self.download_file(link, self.data_folder)
                    self.osm_data_file = self.download_torrent(torrent_file, self.data_folder)
                    self.osm_data_filtered = False

                    os.remove(torrent_file)

                    self.node_list.delete_pickle_file()
                    self.way_list.delete_pickle_file()
                    self.relation_list.delete_pickle_file()
        
        if force:
            self.node_list.delete_pickle_file()
            self.way_list.delete_pickle_file()
            self.relation_list.delete_pickle_file()
        if (not self.node_list.file_exists() and not self.way_list.file_exists() and not self.relation_list.file_exists()):

            filtered_planet = os.path.join(self.data_folder, self.osm_filtered_filename_prefix + date_str + self.osm_extension)
            self.analyse_osm_file(self.atp_sets, self.osm_data_file, filtered_planet, self.osm_data_filtered, self.node_list, self.way_list, self.relation_list)

        timestamp = self.get_osm_file_timestamp()
        if timestamp != None:
            self.osm_timestamp = timestamp
        #if not self.osm_data_filtered:
        #    os.remove(self.osm_data_file)

    def get_osm_file_timestamp(self):
        f = o.io.Reader(self.osm_data_file, o.osm.osm_entity_bits.NOTHING)
        date_str = f.header().get("osmosis_replication_timestamp", "<none>")
        if date_str == '<none>':
            return None
        return date_str

    def analyse_osm_file(self, sets, filename, out_filename, alreadyfiltered, nodes_list, ways_list, relations_list):

        if not alreadyfiltered:
            writer = o.SimpleWriter(out_filename)
        
        brand_matching_key = 'brand:wikidata'
        # only scan the ways of the file
        for obj in o.FileProcessor(filename).with_filter(o.filter.KeyFilter(brand_matching_key)):
            if obj.tags[brand_matching_key] in self.atp_sets:
                matched_spider_name, matched_ref = omta.find_atp_name_and_ref_by_element(self.atp_sets[obj.tags[brand_matching_key]], obj.tags)
            else:
                matched_spider_name, matched_ref = None, None
            if obj.is_node():
                self.process_osm_object(obj, nodes_list, matched_spider_name, matched_ref)
                if not alreadyfiltered:
                    writer.add_node(obj)
            elif obj.is_way():
                self.process_osm_object(obj, ways_list, matched_spider_name, matched_ref)
                if not alreadyfiltered:
                    writer.add_way(obj)
            elif obj.is_relation():
                self.process_osm_object(obj, relations_list, matched_spider_name, matched_ref)
                if not alreadyfiltered:
                    writer.add_relation(obj)

        operator_matching_key = 'operator:wikidata'
        for obj in o.FileProcessor(filename).with_filter(o.filter.KeyFilter(operator_matching_key)):
            if brand_matching_key not in obj.tags:
                if obj.tags[operator_matching_key] in self.atp_sets:
                    matched_spider_name, matched_ref = omta.find_atp_name_and_ref_by_element(self.atp_sets[obj.tags[operator_matching_key]], obj.tags)
                else:
                    matched_spider_name, matched_ref = None, None
                if obj.is_node():
                    self.process_osm_object(obj, nodes_list, matched_spider_name, matched_ref)
                    if not alreadyfiltered:
                        writer.add_node(obj)
                elif obj.is_way():
                    self.process_osm_object(obj, ways_list, matched_spider_name, matched_ref)
                    if not alreadyfiltered:
                        writer.add_way(obj)
                elif obj.is_relation():
                    self.process_osm_object(obj, relations_list, matched_spider_name, matched_ref)
                    if not alreadyfiltered:
                        writer.add_relation(obj)

        nodes_list.save()
        ways_list.save()
        relations_list.save()
                
        if not alreadyfiltered:
            writer.close()
        return

    def process_osm_object(self, obj, set, spider_name, element_ref):
        if spider_name is not None:
            try:
                set.append(obj.id, spider_name, element_ref, obj.timestamp)
            except Exception as e:
                print(e)


    def download_torrent(self, torrent_file, data_path):
        ses = lt.session()
        info = lt.torrent_info(torrent_file)
        h = ses.add_torrent({'ti': info, 'save_path': data_path})
        print(f'Starting download: {h.name()}')

        while not h.is_seed():
            s = h.status()
            print(f'Downloading: {s.progress * 100:.2f}% complete')
            time.sleep(1)

        print('Download complete!')

