import os
from datetime import datetime
from lxml import etree
import geojson
import geopy.distance

directory_path = "./compare_results"
resulting_path = "./geojson"

one_match_colour = '#00FF00'
more_matches_colour = '#0000FF'
no_matches_colour = '#FF0000'
not_matched_osm_colour = '#FFFFFF'
has_matching_tags_but_no_match_colour = '#DDDDDD'

def create_geojson_element(import_element, osm_element, match_colour ):
    
    import_properties = {"marker-color": f"{match_colour}"}
    for tag in import_element.findall('tag'):
        import_properties[tag.attrib['k']] = tag.attrib['v']
    import_coords = (float(import_element.attrib['lon']), float(import_element.attrib['lat']))
    osm_coords = None
    if osm_element != None:
        osm_coords = (float(osm_element.attrib['lon']), float(osm_element.attrib['lat']))
        import_properties['distance'] = str(geopy.distance.geodesic(import_coords, osm_coords).m)
    if osm_element != None and "id" in osm_element.attrib:
        import_properties['osm_link'] = f'https://osm.org/{osm_element.tag}/{osm_element.attrib["id"]}'
    elif import_element != None and "id" in import_element.attrib:
        import_properties['osm_link'] = f'https://osm.org/{import_element.tag}/{import_element.attrib["id"]}'
    return geojson.Feature(geometry=geojson.Point(import_coords), properties=import_properties)
    


def create_geojson(name, xml_dict):
    one_match_list = xml_dict.xpath('/osm/*[not(self::domain) and count(matches/*) = 1]')
    more_matches_list = xml_dict.xpath('/osm/*[not(self::domain) and count(matches/*) > 1]')
    no_matches_list = xml_dict.xpath('/osm/*[not(self::domain) and not(self::matches) and count(matches/*) = 0]')
    not_matched_osm_list = xml_dict.xpath('/osm/matches/child::*')
    result_geojson = geojson.FeatureCollection([])
    for one_match in one_match_list:
        import_feature = create_geojson_element(one_match, one_match.find('./matches/*'), one_match_colour )
        result_geojson['features'].append(import_feature)
    for more_matches in more_matches_list:
        for osm_match in more_matches.find('./matches/*'):
            import_feature = create_geojson_element(more_matches, osm_match, more_matches_colour )
            result_geojson['features'].append(import_feature)
    for no_matches in no_matches_list:
        import_feature = create_geojson_element(no_matches, None, no_matches_colour )
        result_geojson['features'].append(import_feature)
    for not_matched in not_matched_osm_list:
        import_feature = create_geojson_element(not_matched, None, not_matched_osm_colour )
        result_geojson['features'].append(import_feature)
    if not os.path.exists(resulting_path):
        os.makedirs(resulting_path)
    with open(os.path.join(resulting_path,name+".geojson"), "w", encoding='utf-8') as outfile:
        outfile.write(geojson.dumps(result_geojson, indent=2, sort_keys=True))