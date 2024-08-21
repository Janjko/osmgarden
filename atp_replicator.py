import atp_data_load
import replicator
import datetime as dt
import pickle
import os

pickled_sets = "./pickled_sets.bin"

if not os.path.exists(pickled_sets):
    sets = atp_data_load.get_atp_sets('./output')
    with open(pickled_sets, "wb") as f:
        pickle.dump(sets, f)
else:
    with open(pickled_sets, "rb") as f:
        sets = pickle.load(f)
rep_date = dt.datetime.now().replace(tzinfo=dt.timezone.utc)
repserv = replicator.get_server()
seqid = repserv.timestamp_to_sequence(rep_date)
h = replicator.FileStatsHandler(sets, atp_data_load.match_to_set)
maxkb = 10 * 1024
while True:
    lastseqid = repserv.apply_diffs(h, seqid, maxkb)
    if lastseqid != None:
        seqid = lastseqid + 1