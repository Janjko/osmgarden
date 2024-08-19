import os
import json
from geojson import FeatureCollection
import hashlib
import base64

def compute_hash(items) -> str:
    sha1 = hashlib.sha1()
    for item in items:
        sha1.update(item.encode("utf8"))
    return base64.urlsafe_b64encode(sha1.digest()).decode("utf8")

def import_atp_set_matching_tags(path):
    sets = {}
    atm_items = {}
    for filename in os.listdir(path):
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
                    sets[wikidata_value].append(name)
                else:
                    sets[wikidata_value] = [name]
            for atp_ref in temp_atm_items:
                hash = compute_hash([wikidata_value, atp_ref])
                if hash in atm_items:
                    atm_items[hash].append(name)
                else:
                    atm_items[hash] = [name]
            print("Common key-value pairs:", common_tags)

import_atp_set_matching_tags('./output')
print('gotovo!')