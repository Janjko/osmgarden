"""
Simple example that counts the number of changes on a replication server
starting from a given timestamp for a maximum of n hours.

Shows how to detect the different kind of modifications.
"""
import osmium as o
import sys
import datetime as dt
import osmium.replication.server as rserv
from lxml import etree
import os
import json
from datetime import datetime
from collections import namedtuple
from comparer import Comparer, Result
import time
import requests

compare_results_path = "./compare_results"
osm_extracts_folder = "./osm_extracts"
generated_imports_dir = "./import_xml_generated/"
rss_dir = "./rss"

class Stats(object):

    def __init__(self, comparers):
        self.results = []
        self.comparers = comparers

    def process(self, o):
        if o.deleted:
            for comparer in self.comparers:
                if len(o.tags)>0 and comparer.is_match(o.tags):
                    self.results.append(Result(comparer.name, 'del', o.id, o.changeset))
        elif o.version == 1:
            for comparer in self.comparers:
                if len(o.tags)>0 and comparer.is_match(o.tags):
                    self.results.append(Result(comparer.name, 'add', o.id, o.changeset))
        else:
            for comparer in self.comparers:
                if len(o.tags)>0 and comparer.is_match(o.tags):
                    self.results.append(Result(comparer.name, 'mod', o.id, o.changeset))


class FileStatsHandler(o.SimpleHandler):
    def __init__(self, comparers):
        super(FileStatsHandler, self).__init__()
        self.comparers = comparers
        self.nodes = Stats(comparers)
        self.ways = Stats(comparers)
        self.rels = Stats(comparers)

    def node(self, n):
        if n.deleted:
            if len(n.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_deleted(n)
        elif o.version == 1:
            if len(n.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_added(n)
        else:
            for comparer in self.comparers:
                comparer.process_modified_node(n)


    def way(self, w):
        if w.deleted:
            if len(w.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_deleted(w)
        elif o.version == 1:
            if len(w.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_added(w)
        else:
            for comparer in self.comparers:
                comparer.process_modified_way(w)

    def relation(self, r):
        if r.deleted:
            if len(r.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_deleted(r)
        elif o.version == 1:
            if len(r.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_added(r)
        else:
            for comparer in self.comparers:
                comparer.process_modified_relation(r)

def get_fresh_osm_data(xml_doc):
    url = xml_doc.xpath('/osm/domain/@overpass')[0]
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data. Status code: {response.status_code}")
        return None

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python osm_replication_stats.py <server_url> <start_time> <max kB>")
        sys.argv=["", "https://planet.openstreetmap.org/replication/minute/", "2024-08-11T21:21:00Z", "100000"]
    
    comparers = []

    comparer_list = [file_name.rstrip('.xml') for file_name in os.listdir(generated_imports_dir)]

    filenames = os.listdir(compare_results_path)
    file_dict = {}
    for filename in filenames:
        name, date_str = os.path.splitext(filename)[0].split("@")
        date = datetime.strptime(date_str, "%Y-%m-%dT%H_%M_%SZ")
        if name not in file_dict or date > file_dict[name][0]:
            file_dict[name] = [date, filename]

    for comparer_name in comparer_list:
        if comparer_name not in file_dict:
            import_full_path = os.path.join(generated_imports_dir, comparer_name+".xml")
            import_doc = etree.parse(import_full_path)
            overpass_result = get_fresh_osm_data(import_doc)
            overpass_timestamp = overpass_result['osm3s']['timestamp_osm_base']
            new_comparer = Comparer(comparer_name, import_doc, overpass_timestamp, "./compare_results")
            comparers.append(new_comparer)
            new_comparer.fill_base_data_with_overpass_json(overpass_result)
    for import_name, import_date_filename in file_dict.items():

        
        
        overpass_result = get_fresh_osm_data(import_doc)

        tags={}
        for tag in import_doc.findall('domain//tag'):
            tags[tag.attrib['k']] = tag.attrib['v']
        this_comparer = Comparer(import_name, tags, import_doc, import_date_filename[0], "./compare_results")
        this_comparer.fill_base_data_with_overpass_json(overpass_result)
        comparers.append(this_comparer)


    server_url = sys.argv[1]
    start = dt.datetime.strptime(sys.argv[2], "%Y-%m-%dT%H:%M:%SZ")
    if sys.version_info >= (3,0):
        start = start.replace(tzinfo=dt.timezone.utc)
    maxkb = min(int(sys.argv[3]), 10 * 1024)

    repserv = rserv.ReplicationServer(server_url)
    num = 0

    seqid = repserv.timestamp_to_sequence(start)
    print("Initial sequence id:", seqid)

    h = FileStatsHandler(comparers)
    while True:
        lastseqid = repserv.apply_diffs(h, seqid, maxkb)
        if lastseqid != None:
            seqid = lastseqid + 1
        time.sleep(5)
        print("Final sequence id:", lastseqid)
