import requests
from pathlib import Path
import osmium as o
import os
import pickle


def match_element_to_set(element, set):
    qcode = element

pickled_sets = "./pickled_sets.bin"
with open(pickled_sets, "rb") as f:
    sets = pickle.load(f)

url = "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf"
filename = "planet-latest.osm.pbf"
def download_file(url):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

if not os.path.exists(filename):
    filename = download_file(url)
out_filename = 'filtered.osm.pbf'
# go through the ways to find all relevant nodes
writer = o.SimpleWriter(out_filename)
# Pre-filter the ways by tags. The less object we need to look at, the better.

# only scan the ways of the file
for obj in o.FileProcessor(filename).with_filter(o.filter.KeyFilter('brand:wikidata')):
    match_element_to_set(obj, set)
    if obj.is_node():
        writer.add_node(obj)
    elif obj.is_way():
        writer.add_way(obj)
    elif obj.is_relation():
        writer.add_relation(obj)

for obj in o.FileProcessor(filename).with_filter(o.filter.KeyFilter('operator:wikidata')):
    if 'brand:wikidata' not in obj.tags:
        if obj.is_node():
            writer.add_node(obj)
        elif obj.is_way():
            writer.add_way(obj)
        elif obj.is_relation():
            writer.add_relation(obj)


writer.close()