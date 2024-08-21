import osmium.replication.server as rserv
import osmium as o


class FileStatsHandler(o.SimpleHandler):
    def __init__(self, sets, handler):
        super(FileStatsHandler, self).__init__()
        self.handler = handler
        self.sets = sets

    def node(self, n):
        if n.deleted:
            if len(n.tags) > 0:
                self.handler(self.sets, n.tags, 'd')
        elif n.version == 1:
            if len(n.tags) > 0:
                self.handler(self.sets, n.tags, 'a')
        #else:
            #self.handler(self.sets, n.tags, 'm')


    def way(self, w):
        if w.deleted:
            if len(w.tags) > 0:
                self.handler(self.sets, w.tags, 'd')
        elif w.version == 1:
            if len(w.tags) > 0:
                self.handler(self.sets, w.tags, 'a')
        #else:
            #self.handler(self.sets, w.tags, 'm')

    def relation(self, r):
        if r.deleted:
            if len(r.tags) > 0:
                self.handler(self.sets, r.tags, 'd')
        elif r.version == 1:
            if len(r.tags) > 0:
                self.handler(self.sets, r.tags, 'a')
        #else:
            #self.handler(self.sets, r.tags, 'm')

def get_server():
    server_url = "https://planet.openstreetmap.org/replication/minute/"

    return rserv.ReplicationServer(server_url)