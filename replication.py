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

generated_imports_dir = "./import_xml_generated/"
rss_dir = "./rss"
Result = namedtuple('Result', ['comparer_name', 'event', 'type', 'id', 'changeset', 'timestamp'])
class Comparer(object):

    def __init__(self, name, matching_tags, server_url ):
        self.name = name
        self.matching_tags = matching_tags
        self.server_url = server_url

    def is_match(self, tags) -> bool:
        for matching_key in self.matching_tags.keys():
            if matching_key not in tags:
                return False
        return True



class Stats(object):

    def __init__(self, comparers):
        self.results = []
        self.comparers = comparers

    def process(self, o):
        if o.deleted:
            for comparer in self.comparers:
                if len(o.tags)>0 and comparer.is_match(o.tags):
                    self.results.append(Result(comparer.name, 'del', o.id, o.changeset, o.timestamp))
        elif o.version == 1:
            print(f"Add   : {o.timestamp.strftime('%Y-%m-%d')} - {str(o.tags)}")
        else:
            print(f"Modify: {o.timestamp.strftime('%Y-%m-%d')} - {str(o.tags)}")

    def outstats(self, prefix):
        print("%s added: %d" % (prefix, self.added))
        print("%s modified: %d" % (prefix, self.modified))
        print("%s deleted: %d" % (prefix, self.deleted))

class FileStatsHandler(o.SimpleHandler):
    def __init__(self, comparers):
        super(FileStatsHandler, self).__init__()
        self.nodes = Stats(comparers)
        self.ways = Stats(comparers)
        self.rels = Stats(comparers)

    def node(self, n):
        self.nodes.process(n)

    def way(self, w):
        self.ways.process(w)

    def relation(self, r):
        self.rels.process(r)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python osm_replication_stats.py <server_url> <start_time> <max kB>")
        sys.argv=["", "https://download.geofabrik.de/europe/croatia-updates/", "2024-08-03T19:00:00Z", "100000"]

    server_url = sys.argv[1]
    start = dt.datetime.strptime(sys.argv[2], "%Y-%m-%dT%H:%M:%SZ")
    if sys.version_info >= (3,0):
        start = start.replace(tzinfo=dt.timezone.utc)
    maxkb = min(int(sys.argv[3]), 10 * 1024)

    repserv = rserv.ReplicationServer(server_url)

    seqid = repserv.timestamp_to_sequence(start)
    print("Initial sequence id:", seqid)

    for filename in os.listdir(generated_imports_dir):
        if filename.endswith('.xml'):
            name = filename.rstrip('.xml')
            full_path = os.path.join(generated_imports_dir, filename)
            xml_doc = etree.parse(full_path)
            
    h = FileStatsHandler([Comparer("konzum_hr", {"brand:wikidata": "Q518563", "shop": "convenience"}, "http:cro")])
    seqid = repserv.apply_diffs(h, seqid, maxkb)
    print("Final sequence id:", seqid)

    h.nodes.outstats("Nodes")
    h.ways.outstats("Ways")
    h.rels.outstats("Relations")