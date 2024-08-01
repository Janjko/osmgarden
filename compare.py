import requests
import os
from lxml import etree
import io
import hashlib
import base64
from geojson import FeatureCollection, Feature, Point
import geojson
import geopy.distance
from pathlib import Path

one_match_colour = '#00FF00'
more_matches_colour = '#0000FF'
no_matches_colour = '#FF0000'
not_matched_osm_colour = '#FFFFFF'
has_matching_tags_but_no_match_colour = '#DDDDDD'

# Right now only overpass querys
def get_fresh_osm_data(xml_data):
    url = xml_doc.xpath('/osm/domain/@overpass')[0]
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data. Status code: {response.status_code}")
        return None

def compute_hash(ref, spider_name) -> str:
    sha1 = hashlib.sha1()
    sha1.update(ref.encode("utf8"))
    sha1.update(spider_name.encode("utf8"))
    return base64.urlsafe_b64encode(sha1.digest()).decode("utf8")

def compare_atp_data(import_xml, osm_object, name):
    spiders = import_xml.xpath('//@atp-spider')
    spiders = list( dict.fromkeys(spiders) )
    import_elements = import_xml.xpath('/osm/child::*')
    matching_tag_keys = get_matching_tags(import_elements)

    for osm_element in osm_object['elements']:
        if not('ref' in osm_element['tags']):
            continue
        hashes = []
        for spider in spiders:
            hashes.append(compute_hash(osm_element['tags']['ref'], spider))
        osm_element['hashes'] = hashes
    result_geojson = FeatureCollection([])
    for import_element in import_elements:
        if import_element.tag == "domain":
            continue
        matching_tags = import_element.xpath("./tag[@function='match']")
        matching_elements = search_matching_osm_elements(osm_object, matching_tags)
        if len(matching_elements) > 0:
            matches_xml = etree.SubElement(import_element, 'matches')
        for matching_element in matching_elements:
            etree.SubElement(matches_xml, matching_element['type'],
                                          id=str(matching_element['id']),
                                          version=str(matching_element['version']),
                                          changeset=str(matching_element['changeset']))
            
        import_coords = get_element_coordinates(import_element, True)
        match_colour = no_matches_colour
        if len(matching_elements) == 1:
            match_colour = one_match_colour
        if len(matching_elements) > 1:
            match_colour = more_matches_colour
        import_properties  = {"match": f"{len(matching_elements)}", "marker-color": f"{match_colour}"}
        for tag in import_element.findall('tag'):
            import_properties[tag.attrib['k']] = tag.attrib['v']
        matching_element_urls = []
        distance = None
        for matching_element in matching_elements:
            osm_coords = get_element_coordinates(matching_element, False)
            temp_distance = geopy.distance.geodesic(import_coords, osm_coords).m
            if distance == None or distance > temp_distance:
                distance = temp_distance
            matching_element_urls.append(f'https://osm.org/{matching_element["type"]}/{matching_element["id"]}')
        import_properties['osm_link'] = matching_element_urls
        import_properties['distance'] = str(distance)
        import_feature = Feature(geometry=Point(import_coords), properties=import_properties)
        result_geojson['features'].append(import_feature)
    for osm_element in osm_object['elements']:
        if 'matched' not in osm_element or osm_element['matched'] != 'yes':
            osm_point = Point(get_element_coordinates(osm_element, False))
            osm_properties  = { "marker-color": f"{not_matched_osm_colour}"}
            for tag_combination in matching_tag_keys:
                if all(key in osm_element['tags'].keys() for key in tag_combination):
                    osm_properties  = { "marker-color": f"{has_matching_tags_but_no_match_colour}"}

            for tagkey, tagvalue in osm_element['tags'].items():
                osm_properties[tagkey] = tagvalue
            osm_properties['osm_link'] = f'https://osm.org/{osm_element["type"]}/{osm_element["id"]}'
            result_geojson['features'].append(Feature(geometry=osm_point, properties=osm_properties))
    with open("my_points.geojson", "w", encoding='utf-8') as outfile:
        outfile.write(geojson.dumps(result_geojson, indent=2, sort_keys=True))
    xml_file_name = name + '@' + import_xml.getroot().attrib['timestamp_osm_base'].replace(":", "_")
    Path("./compare_results").mkdir(parents=True, exist_ok=True)
    import_xml.write(f"compare_results/{xml_file_name}.xml", pretty_print=True, xml_declaration=True, encoding="UTF-8")

def get_element_coordinates(element, isxml):
    my_point = None
    if isxml:
        return (float(element.attrib['lon']), float(element.attrib['lat']))
    else:
        if element['type'] == 'node':
            return (float(element['lon']), float(element['lat']))
        else:
            return (float(element['center']['lon']), float(element['center']['lat']))

def search_matching_osm_elements(jsonobj, matching_tags):
    elements = []
    key_value_pairs = [(elem.get("k"), elem.get("v")) for elem in matching_tags]
    for element in jsonobj['elements']:
        if all(key in element['tags'] and element['tags'][key] == value for key, value in key_value_pairs):
            element['matched'] = 'yes'
            elements.append(element)
    return elements

def get_matching_tags(import_elements):
    matching_tags = []
    for import_element in import_elements:
        temp_matching_tags = []
        if import_element.tag == "domain":
            continue
        temp_matching_tags = sorted(import_element.xpath("./tag[@function='match']/@k"))
        if (not list_of_lists_contains_list( matching_tags, temp_matching_tags )):
            matching_tags.append(temp_matching_tags.copy())
    return matching_tags

def list_of_lists_contains_list(a, b):
    for i in a:
        if i == b:
            return True

directory_path = './import_xml_generated'
for filename in os.listdir(directory_path):
    if filename.endswith('.xml'):
        name = filename.rstrip('.xml')
        full_path = os.path.join(directory_path, filename)
        xml_doc = etree.parse(full_path)
        osm_object = get_fresh_osm_data(xml_doc)
        xml_doc.getroot().attrib['timestamp_osm_base'] = osm_object['osm3s']['timestamp_osm_base']
        xml_doc.getroot().attrib['timestamp_areas_base'] = osm_object['osm3s']['timestamp_areas_base']
        compare_atp_data(xml_doc, osm_object, name)
