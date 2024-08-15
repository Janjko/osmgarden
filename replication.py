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

class FileStatsHandler(o.SimpleHandler):
    def __init__(self, comparers):
        super(FileStatsHandler, self).__init__()
        self.comparers = comparers

    def node(self, n):
        if n.deleted:
            if len(n.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_deleted_node(n)
        elif n.version == 1:
            if len(n.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_added_node(n)
        else:
            for comparer in self.comparers:
                comparer.process_modified_node(n)


    def way(self, w):
        if w.deleted:
            if len(w.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_deleted_way(w)
        elif w.version == 1:
            if len(w.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_added_way(w)
        else:
            for comparer in self.comparers:
                comparer.process_modified_way(w)

    def relation(self, r):
        if r.deleted:
            if len(r.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_deleted_relation(r)
        elif r.version == 1:
            if len(r.tags) > 0:
                for comparer in self.comparers:
                    comparer.process_added_relation(r)
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

    server_url = "https://planet.openstreetmap.org/replication/minute/"

    maxkb = 10 * 1024

    repserv = rserv.ReplicationServer(server_url)
    comparers = []

    comparer_list = [file_name.rstrip('.xml') for file_name in os.listdir(generated_imports_dir)]

    filenames = os.listdir(compare_results_path)
    compare_result_file_dict = {}
    for filename in filenames:
        name, date_str = os.path.splitext(filename)[0].split("@")
        date = datetime.strptime(date_str, "%Y-%m-%dT%H_%M_%SZ")
        if name not in compare_result_file_dict or date > compare_result_file_dict[name][0]:
            compare_result_file_dict[name] = [date, filename]

    seqid_list = []

    for comparer_name in comparer_list:
        if comparer_name not in compare_result_file_dict:
            import_full_path = os.path.join(generated_imports_dir, comparer_name+".xml")
            import_doc = etree.parse(import_full_path)
            overpass_result = get_fresh_osm_data(import_doc)
            overpass_timestamp = dt.datetime.strptime(overpass_result['osm3s']['timestamp_osm_base'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
            seqid = repserv.timestamp_to_sequence(overpass_timestamp)
            seqid_list.append(seqid)
            new_comparer = Comparer(comparer_name, import_doc, overpass_timestamp, seqid, compare_results_path)
            comparers.append(new_comparer)
            new_comparer.fill_base_data_with_overpass_json(overpass_result)
        else:
            compare_results_filename = os.path.join(compare_results_path, compare_result_file_dict[comparer_name][1] )
            import_doc = etree.parse(compare_results_filename)
            import_doc_timestamp = dt.datetime.strptime(import_doc.getroot().attrib['timestamp_osm_base'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
            seqid = int(import_doc.getroot().attrib['seqid'])
            seqid_list.append(seqid)
            existing_comparer = Comparer(comparer_name, import_doc, import_doc_timestamp, seqid, compare_results_path)
            comparers.append(existing_comparer)



    seqid = min(seqid_list)
    print("Initial sequence id:", seqid)

    h = FileStatsHandler(comparers)
    while True:
        lastseqid = repserv.apply_diffs(h, seqid, maxkb)
        for comparer in comparers:
            if comparer.change_count > 0:
                comparer.write_compare_result()
                comparer.change_count = 0
        if lastseqid != None:
            seqid = lastseqid + 1
        time.sleep(5)
        print("Final sequence id:", lastseqid)
