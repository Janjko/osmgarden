import pickle
from collections import namedtuple
import os

OSM_Set = namedtuple('OSM_Set', ['name', 'element', 'timestamp', 'version'])

class AutoSaveArray:
    def __init__(self, filename):
        # Create a named tuple with the name 'OSM_Set' and fields 'name' and 'element'
        self.filename = filename
        self.load_pickle_file()
        self._observers = []
        
    def load_pickle_file(self):
        try:
            with open(self.filename, 'rb') as f:
                self.data = pickle.load(f)
        except (FileNotFoundError, EOFError):
            self.data = {}

    def append(self, osm_id, name, element, timestamp, version):
        # Create a new named tuple and append to the data array
        record = OSM_Set(name, element, timestamp, version)
        if osm_id in self.data:
            raise Exception ("Element already in set.")
        self.data[osm_id] = record

    def update_and_save(self, osm_id, name, element, timestamp, version):
        record = OSM_Set(name, element, timestamp, version)
        if self.data[osm_id] == record:
            raise Exception ("Updating element when it was already like that.")
        old_data_version = self.data[osm_id].version
        self.data[osm_id] = record
        if old_data_version != version-1:
            raise Exception (f"Version isn't incremented by one. Version before was {self.data[osm_id].version}, and version now is {version}.")

        self.save_and_notify(timestamp)

    def append_and_save(self, osm_id, name, element, timestamp, version):
        self.append(osm_id, name, element, timestamp, version)
        self.save_and_notify(timestamp)

    def delete_and_save(self, osm_id, timestamp, version):
        if osm_id not in self.data:
            raise Exception ("Element not in set, can't delete.")
        old_data_version = self.data[osm_id].version
        self.data.pop(osm_id)
        self.save_and_notify(timestamp)
        if old_data_version >= version:
            raise Exception (f"Version isn't incremented. Version before was {old_data_version}, and version now is {version}.")

    def save_and_notify(self, timestamp):
        with open(self.filename, 'wb') as f:
            pickle.dump(self.data, f)
        self.notify_observers(timestamp)

    def save(self):
        with open(self.filename, 'wb') as f:
            pickle.dump(self.data, f)

    def get_all(self):
        # Return all records in the array
        return self.data
    
    def file_exists(self):
        return os.path.exists(self.filename)
    
    def delete_pickle_file(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)
        self.load_pickle_file()

    def register_observer(self, observer):
        self._observers.append(observer)

    def notify_observers(self, timestamp):
        for observer in self._observers:
            observer.update(timestamp)

