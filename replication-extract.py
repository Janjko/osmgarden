import osmium as o
import sys
import os
from lxml import etree
from collections import namedtuple
import requests
from comparer import Comparer, Result

generated_imports_dir = "./import_xml_generated/"
Result = namedtuple('Result', ['osm_type', 'id', 'changeset', 'timestamp', 'version', 'tags', 'lat', 'lon'])
osm_extracts_folder = "./osm_extracts"
compare_results_folder = "./compare_results"



class FileListHandler(o.SimpleHandler):
    def __init__(self, comparers):
        super(FileListHandler, self).__init__()
        self.comparers = comparers

    def node(self, n):
        for comparer in self.comparers:
            comparer.process(n, 'node')
            
    def way(self, w):
        for comparer in self.comparers:
            comparer.process(w, 'way')
            
    def relation(self, r):
        for comparer in self.comparers:
            comparer.process(r, 'relation')

def download_file(url, file_path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return file_path

def get_osm_file_timestamp(osm_file):
    f = o.io.Reader(osm_file, o.osm.osm_entity_bits.NOTHING)
    return f.header().get("osmosis_replication_timestamp", "<none>")

def main(osmfile, comparers):

    handler = FileListHandler(comparers)

    handler.apply_file(osmfile, locations=True)

    return 0

if __name__ == '__main__':
    comparers = []
    if not os.path.exists(osm_extracts_folder):
        os.makedirs(osm_extracts_folder)
    if not os.path.exists(compare_results_folder):
        os.makedirs(compare_results_folder)
    for import_filename in os.listdir(generated_imports_dir):
        if import_filename.endswith('.xml'):
            import_name = import_filename.rstrip('.xml')
            import_full_path = os.path.join(generated_imports_dir, import_filename)
            import_doc = etree.parse(import_full_path)
            osm_name = import_doc.find('domain//locationArea').attrib['name'].strip('/').lower() + "-latest.osm.pbf"
            osm_file_name = osm_name.split('/')[1]
            osm_file_url = "https://download.geofabrik.de/" + osm_name
            osm_file_path = os.path.join(osm_extracts_folder, osm_file_name)
            if not os.path.exists(osm_file_path):
                download_file(osm_file_url, osm_file_path)
            osm_timestamp = get_osm_file_timestamp(osm_file_path)
            tags={}
            for tag in import_doc.findall('domain//tag'):
                tags[tag.attrib['k']] = tag.attrib['v']
            comparers.append(Comparer(import_name, tags, import_doc, osm_timestamp))

    main(osm_file_path, comparers)
    for comparer in comparers:
        comparer.match_results()
        comparer.publish_xml()

    print('uspjeh')