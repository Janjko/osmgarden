import osmium.replication.server as rserv
import osmium as o


class FileStatsHandler(o.SimpleHandler):
    def __init__(self, atp_elements, handler, nodes, ways, relations):
        super(FileStatsHandler, self).__init__()
        self.handler = handler
        self.atp_elements = atp_elements
        self.nodes = nodes
        self.ways = ways
        self.relations = relations

    def node(self, n):
        if n.deleted:
            if len(n.tags) > 0:
                self.handler(self.atp_elements, n, 'd', self.nodes)
        elif n.version == 1:
            if len(n.tags) > 0:
                self.handler(self.atp_elements, n, 'a', self.nodes)
        else:
            self.handler(self.atp_elements, n, 'm', self.nodes)


    def way(self, w):
        if w.deleted:
            if len(w.tags) > 0:
                self.handler(self.atp_elements, w, 'd', self.ways)
        elif w.version == 1:
            if len(w.tags) > 0:
                self.handler(self.atp_elements, w, 'a', self.ways)
        else:
            self.handler(self.atp_elements, w, 'm', self.ways)

    def relation(self, r):
        if r.deleted:
            if len(r.tags) > 0:
                self.handler(self.atp_elements, r, 'd', self.relations)
        elif r.version == 1:
            if len(r.tags) > 0:
                self.handler(self.atp_elements, r, 'a', self.relations)
        else:
            self.handler(self.atp_elements, r, 'm', self.relations)

def get_server():
    server_url = "https://planet.openstreetmap.org/replication/minute/"

    return rserv.ReplicationServer(server_url)