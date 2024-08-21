import os
import json
from geojson import FeatureCollection
import hashlib
import base64
from collections import namedtuple

ATP_Set = namedtuple('ATP_Set', ['name', 'elements'])

def compute_hash(items) -> str:
    sha1 = hashlib.sha1()
    for item in items:
        sha1.update(item.encode("utf8"))
    return base64.urlsafe_b64encode(sha1.digest()).decode("utf8")

def get_atp_sets(path):
    sets = {}
    for filename in os.listdir(path):
        #if str.startswith(filename, 'b'):
        #    break
        with open(os.path.join(path, filename), "r") as f:
            try:
                atp_object = json.load(f)
            except:
                print (f"File {filename} invalid")
                continue
            name = atp_object['dataset_attributes']['@spider']
            common_tags = {}
            temp_atm_items = []
            for feature in atp_object["features"]:
                if 'ref' not in feature['properties']:
                    continue
                properties = {key:value
                              for key, value in feature['properties'].items()
                              if key == 'brand:wikidata' or
                              (key=='operator:wikidata' and 'brand:wikidata' not in feature['properties'])}
                if not common_tags:
                    # Initialize common_properties with the first feature's properties
                    common_tags = properties
                else:
                    # Update common_properties with common key-value pairs
                    common_tags = {
                        key: value
                        for key, value in properties.items()
                        if key in common_tags and value == common_tags[key]
                    }
                temp_atm_items.append(feature['properties']['ref'])
            if (len(common_tags)==1):
                wikidata_value = common_tags[next(iter(common_tags))]
                if wikidata_value in sets:
                    sets[wikidata_value].append(ATP_Set(name, temp_atm_items))
                else:
                    sets[wikidata_value] = [ATP_Set(name, temp_atm_items)]
            print("Common key-value pairs:", common_tags)
    return sets

def match_to_set(sets, tags, action):
    brand_key = 'brand:wikidata'
    operator_key = 'operator:wikidata'
    ref_key = 'ref'
    matched_key = None
    if brand_key in tags and tags[brand_key] in sets:
        matched_key = brand_key
    elif operator_key in tags and tags[operator_key] in sets:
        matched_key = operator_key
    if matched_key is not None:
        if ref_key in tags and tags[ref_key] in sets[tags[matched_key]].elements:
            print(f"Matched element from set {sets[tags[matched_key]][0].name} and matched ref {tags[ref_key]}, action {action}" )
        else:
            print(f"Matched element from set {sets[tags[matched_key]][0].name} but ref not matched, action {action}" )
        