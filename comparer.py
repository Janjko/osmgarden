from lxml import etree
from collections import namedtuple
import os
import osmium as o
import datetime as dt

Result = namedtuple('Result', ['type', 'id', 'changeset', 'version', 'tags', 'lat', 'lon'])

class Comparer(object):
    def __init__(self, name, import_doc: etree.ElementTree, timestamp, seqid, compare_results_folder):
        self.matches = []
        self.name = name
        self.import_doc = import_doc
        self.matching_tags = dict([(foundtag.attrib['k'], foundtag.attrib['v']) for foundtag in import_doc.findall('domain//tag')])
        self.import_elements = import_doc.xpath('/osm/child::*[not(self::domain)]')
        self.import_osm_node_ids = [node.attrib['id'] for node in self.import_doc.findall('.//node')]
        self.import_osm_way_ids = [node.attrib['id'] for node in self.import_doc.findall('.//way')]
        self.import_osm_relation_ids = [node.attrib['id'] for node in self.import_doc.findall('.//relation')]
        self.timestamp = timestamp
        self.compare_results_folder = compare_results_folder
        self.seqid = seqid
        import_doc.getroot().attrib['seqid'] = str(self.seqid)

    def is_match(self, tags) -> bool:
        for key, value in self.matching_tags.items():
            if key not in tags or tags[key]!=value:
                return False
        return True

    def process(self, o, osm_type):
        if len(o.tags) >= len(self.matching_tags) and self.is_match(o.tags):
            element_location = self.get_lat_lon(o, osm_type)
            self.matches.append(Result(type=osm_type,
                                       id=o.id,
                                       changeset=o.changeset,
                                       timestamp=o.timestamp,
                                       version=o.version,
                                       tags=dict(o.tags),
                                       lat=element_location[0],
                                       lon=element_location[1]  ))

    def match_osm_element(self, osm_el):
        matched = False
        for import_element in self.import_elements:
            matching_tags = import_element.findall("./tag[@function='match']")
            key_value_pairs = [(elem.get("k"), elem.get("v")) for elem in matching_tags]
            if all(key in osm_el.tags and osm_el.tags[key] == value for key, value in key_value_pairs):
                self.add_to_matching_elements(import_element, osm_el.type, osm_el.id, osm_el.version, osm_el.lat, osm_el.lon)
                matched = True
                break
        if matched != True:
                self.add_to_matching_elements(import_element.getparent(), osm_el.type, osm_el.id, osm_el.version, osm_el.lat, osm_el.lon)

    def match_results(self):
        for osm_el in self.matches:
            self.match_osm_element(osm_el)

    def add_to_matching_elements(self, xml_element, osm_type, osm_id, osm_version, osm_lat, osm_lon):
        matches_xml = xml_element.find('matches')
        if matches_xml == None:
            matches_xml = etree.SubElement(xml_element, 'matches')
        etree.SubElement(matches_xml, osm_type,
                                    id=str(osm_id),
                                    version=str(osm_version),
                                    lat=str(osm_lat),
                                    lon=str(osm_lon))

    def publish_xml(self):
        xml_file_name = self.name + '@' + self.timestamp.replace(":", "_")
        xml_file_path = os.path.join(self.compare_results_folder, xml_file_name + ".xml")
        self.import_doc.write(xml_file_path, xml_declaration=True, encoding="UTF-8")
    
    def process_deleted_node(self, o):
        self.process_modified(o, self.import_osm_node_ids, "node")
    def process_deleted_way(self, o):
        self.process_modified(o, self.import_osm_way_ids, "way")
    def process_deleted_relation(self, o):
        self.process_modified(o, self.import_osm_relation_ids, "relation")  
    def process_deleted(self, o, import_osm_ids: list, osm_type: str):
        if self.is_match(o.tags):
            self.remove_match(self, osm_type, o.id)
            import_osm_ids.remove(o.id)
    
    def process_added_node(self, o):
        self.process_modified(o, self.import_osm_node_ids, "node", self.get_lat_lon_node(o))
    def process_added_way(self, o):
        self.process_modified(o, self.import_osm_way_ids, "way", self.get_lat_lon_way(o))
    def process_added_relation(self, o):
        self.process_modified(o, self.import_osm_relation_ids, "relation", self.get_lat_lon_relation(o))    
    def process_added(self, o, import_osm_ids: list, osm_type: str, location):
        if self.is_match(o.tags):
            self.match_osm_element(Result(id=o.id,
                                          type=osm_type,
                                          changeset=o.changeset,
                                          tags=dict(o.tags),
                                          version=o.version,
                                          lat=location[0],
                                          lon=location[1]))
            print (f"OSM Element id {osm_type} {id} added")
            import_osm_ids.append(o.id)
    
    def process_modified_node(self, o):
        self.process_modified(o, "node", self.import_osm_node_ids, [o.location.lat, o.location.lon])
    def process_modified_way(self, o):
        self.process_modified(o, "way", self.import_osm_way_ids, [0, 0])
    def process_modified_relation(self, o):
        self.process_modified(o, "relation", self.import_osm_relation_ids, [0,0])
    def process_modified(self, o, osm_type, import_osm_ids: list, location):
        osm_elem = Result(type=osm_type, id=o.id,
                              version=o.version,
                              changeset=o.changeset,
                              tags=dict(o.tags),
                              lat=location[0],
                              lon=location[1]  )
        if o.id in import_osm_ids:
            if self.is_match(osm_elem.tags):
                print (f"OSM Element id {osm_elem.type} {osm_elem.id} changed")
            else:
                self.remove_match(osm_elem.type, osm_elem.id)
                print (f"OSM Element id {osm_elem.type} {osm_elem.id} changed, and is not in set anymore")
        elif len(osm_elem.tags) > 0 and self.is_match(osm_elem.tags):
            self.match_osm_element(osm_elem)
            print (f"OSM Element id {osm_elem.type} {osm_elem.id} changed and so came to the set")
    
    def remove_match(self, osm_type, id):
        element_to_delete = self.import_doc.getroot().find(f"//{osm_type}[@id='{id}']")
        print (f"OSM Element id {osm_type} {id} changed")
        if len(element_to_delete.getparent().getchildren())==1:
            element_to_delete.getparent().getparent().remove(element_to_delete.getparent())
        else:
            element_to_delete.getparent().remove(element_to_delete)

    def get_lat_lon(self, o, osm_type):
        if osm_type == 'node':
            return [o.location.lat, o.location.lon]
        elif osm_type == 'way':
            return [o.nodes[0].location.lat, o.nodes[0].location.lon]
        else:
            return [0,0]
    def get_lat_lon_node(self, o):
        return [o.location.lat, o.location.lon]
    def get_lat_lon_way(self, o):
        return [o.nodes[0].location.lat, o.nodes[0].location.lon]
    def get_lat_lon_relation(self, o):
        return [0,0]
    
    def fill_base_data_with_overpass_json(self, osm_object):
        date_str = osm_object['osm3s']['timestamp_osm_base']
        self.timestamp = dt.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
        self.import_doc.getroot().attrib['timestamp_osm_base'] = date_str
        for osm_element in osm_object['elements']:
            if osm_element['type']=='node':
                lat = osm_element['lat']
                lon = osm_element['lon']
            else:
                lat = osm_element['center']['lat']
                lon = osm_element['center']['lon']
            osm_el = Result(osm_element['type'], osm_element['id'], osm_element['changeset'], osm_element['version'], osm_element['tags'], lat, lon)
        
            self.match_osm_element(osm_el)
        
        self.write_compare_result()

    def write_compare_result(self):
        file_name = self.name + '@' + self.import_doc.getroot().attrib['timestamp_osm_base'].replace(":", "_")+".xml"
        path = os.path.join(self.compare_results_folder, file_name )
        self.import_doc.write(path, pretty_print=True, xml_declaration=True, encoding="UTF-8")