import osmium.replication.server as rserv
import osmium as o


class FileStatsHandler(o.SimpleHandler):
    def __init__(self, sets, handler, nodes, ways, relations):
        super(FileStatsHandler, self).__init__()
        self.handler = handler
        self.sets = sets
        self.nodes = nodes
        self.ways = ways
        self.relations = relations

    def node(self, n):
        if n.deleted:
            if len(n.tags) > 0:
                self.handler(self.sets, n, 'd', self.nodes)
        elif n.version == 1:
            if len(n.tags) > 0:
                self.handler(self.sets, n, 'a', self.nodes)
        else:
            self.handler(self.sets, n, 'm', self.nodes)


    def way(self, w):
        if w.deleted:
            if len(w.tags) > 0:
                self.handler(self.sets, w, 'd', self.ways)
        elif w.version == 1:
            if len(w.tags) > 0:
                self.handler(self.sets, w, 'a', self.ways)
        else:
            self.handler(self.sets, w, 'm', self.ways)

    def relation(self, r):
        if r.deleted:
            if len(r.tags) > 0:
                self.handler(self.sets, r, 'd', self.relations)
        elif r.version == 1:
            if len(r.tags) > 0:
                self.handler(self.sets, r, 'a', self.relations)
        else:
            self.handler(self.sets, r, 'm', self.relations)

def get_server():
    server_url = "https://planet.openstreetmap.org/replication/minute/"

    return rserv.ReplicationServer(server_url)