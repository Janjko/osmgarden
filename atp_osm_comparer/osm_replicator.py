import osm_matching_to_atp as omta
import osmium_replicator
import datetime as dt
import time
import os
import WebManager



class OsmReplicator:
    def __init__(self, atp_elements, osm_date, data_path, nodes, ways, relations, web_manager: WebManager):
        self.repserv = osmium_replicator.get_server()
        self.atp_elements = atp_elements
        self.osm_date = osm_date
        self.data_path = data_path
        self.nodes = nodes
        self.ways = ways
        self.relations = relations
        self.seqid_path = os.path.join(self.data_path, 'seqid.txt')
        self.seqid = self.get_seqid(self.osm_date, self.seqid_path, self.repserv)
        self.maxkb = 10 * 1024
        self.handler = osmium_replicator.FileStatsHandler(self.atp_elements, omta.match_to_set, self.nodes, self.ways, self.relations)
        self.web_manager = web_manager

    def start_replicator(self):
        next_seqid = self.seqid + 1
        self.web_manager.seqid = next_seqid
        processed_seqid = self.repserv.apply_diffs(self.handler, next_seqid, self.maxkb)
        print(str(processed_seqid))
        if processed_seqid != None:
            self.write_seqid(processed_seqid)
        return processed_seqid
        

    def get_seqid(self, osm_date, filepath, repserv):
        if osm_date == None:
            if not os.path.exists(filepath):
                print("Can't get seqid, no date and seqid file was not found.")
            try:
                with open(filepath, 'r') as file:
                    first_line = file.readline().strip()
                    seqid = int(first_line)
                    return seqid
            except ValueError:
                print("The first line is not a valid integer.")
            except FileNotFoundError:
                print("The file was not found.")
        else:
            rep_date = dt.datetime.strptime(osm_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
            seqid = repserv.timestamp_to_sequence(rep_date)
            self.write_seqid(seqid, filepath)
            return seqid

        
    def write_seqid(self, seqid):
        self.seqid = seqid
        with open(self.seqid_path, 'w') as f:
            f.write(str(seqid))
