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
                self.handler(self.sets, n.tags, 'd', self.nodes)
        elif n.version == 1:
            if len(n.tags) > 0:
                self.handler(self.sets, n.tags, 'a', self.nodes)
        #else:
            #self.handler(self.sets, n.tags, 'm', self.nodes)


    def way(self, w):
        if w.deleted:
            if len(w.tags) > 0:
                self.handler(self.sets, w.tags, 'd', self.ways)
        elif w.version == 1:
            if len(w.tags) > 0:
                self.handler(self.sets, w.tags, 'a', self.ways)
        #else:
            #self.handler(self.sets, w.tags, 'm', self.ways)

    def relation(self, r):
        if r.deleted:
            if len(r.tags) > 0:
                self.handler(self.sets, r.tags, 'd', self.relations)
        elif r.version == 1:
            if len(r.tags) > 0:
                self.handler(self.sets, r.tags, 'a', self.relations)
        #else:
            #self.handler(self.sets, r.tags, 'm', self.relations)

def get_server():
    server_url = "https://planet.openstreetmap.org/replication/minute/"

    return rserv.ReplicationServer(server_url)