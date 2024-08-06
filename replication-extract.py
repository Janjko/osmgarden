import osmium as o
import sys
import os
from lxml import etree
from collections import namedtuple
import requests

generated_imports_dir = "./import_xml_generated/"
Result = namedtuple('Result', ['osm_type', 'id', 'changeset', 'timestamp', 'version', 'tags', 'matched'])
osm_extracts_folder = "./osm_extracts"
compare_results_folder = "./compare_results"

class Comparer(object):
    def __init__(self, name, matching_tags, import_doc, timestamp):
        self.results = []
        self.name = name
        self.matching_tags = matching_tags
        self.import_doc = import_doc
        self.import_elements = import_doc.xpath('/osm/child::*')
        self.timestamp = timestamp

    def is_match(self, tags) -> bool:
        for key, value in self.matching_tags.items():
            if key not in tags or tags[key]!=value:
                return False
        return True

    def process(self, o, osm_type):
        if len(o.tags) >= len(self.matching_tags) and self.is_match(o.tags):
            matched = False
            for import_element in self.import_elements:
                if import_element.tag == "domain":
                    continue
                matching_tags = import_element.findall("./tag[@function='match']")
                key_value_pairs = [(elem.get("k"), elem.get("v")) for elem in matching_tags]
                if all(key in o.tags and o.tags[key] == value for key, value in key_value_pairs):
                    self.add_to_matching_elements(import_element, osm_type, o.id, o.version)
                    continue
            if matched != True:
                    self.add_to_matching_elements(import_element.getparent(), osm_type, o.id, o.version)
            #self.results.append(Result(osm_type, o.id, o.changeset, o.timestamp, o.version, dict(o.tags), matched))
    
    def add_to_matching_elements(self, xml_element, osm_type, osm_id, osm_version):
        matches_xml = xml_element.find('matches')
        if matches_xml == None:
            matches_xml = etree.SubElement(xml_element, 'matches')
        etree.SubElement(matches_xml, osm_type,
                                    id=str(osm_id),
                                    version=str(osm_version))

    def publish_xml(self):
        xml_file_name = self.name + '@' + self.timestamp.replace(":", "_")
        xml_file_path = os.path.join(compare_results_folder, xml_file_name + ".xml")
        self.import_doc.write(xml_file_path, xml_declaration=True, encoding="UTF-8")

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

    handler.apply_file(osmfile)

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
        comparer.publish_xml()

    print('uspjeh')