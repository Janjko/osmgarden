import osm_matching_to_atp as omta
import osmium_replicator
import datetime as dt
import time
import os

seqid_path = 'seqid.txt'

def start_replicator(atp_elements, osm_date, data_path, nodes, ways, relations):
    repserv = osmium_replicator.get_server()
    filepath = data_path + seqid_path
    seqid = get_seqid(osm_date, filepath, repserv)

    h = osmium_replicator.FileStatsHandler(atp_elements, omta.match_to_set, nodes, ways, relations)
    maxkb = 10 * 1024
    while True:
        lastseqid = repserv.apply_diffs(h, seqid, maxkb)
        print(str(lastseqid))
        if lastseqid != None:
            write_seqid(lastseqid, filepath)
            seqid = lastseqid + 1
        else:
            time.sleep(10)

def get_seqid(osm_date, filepath, repserv):
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
        write_seqid(seqid, filepath)
        return seqid

    
def write_seqid(seqid, path):
    with open(path, 'w') as f:
        f.write(str(seqid))
