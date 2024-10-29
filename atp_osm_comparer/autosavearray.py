import pickle
from collections import namedtuple
import os

OSM_Set = namedtuple('OSM_Set', ['name', 'element'])

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

    def append(self, osm_id, name, element):
        # Create a new named tuple and append to the data array
        record = OSM_Set(name, element)
        if osm_id in self.data:
            raise Exception ("Element already in set.")
        self.data[osm_id] = record

    def update_and_save(self, osm_id, name, element):
        record = OSM_Set(name, element)
        if self.data[osm_id] == record:
            raise ("Updating element when it was already like that.")
        self.data[osm_id] = record

        self.save()

    def append_and_save(self, osm_id, name, element):
        self.append(osm_id, name, element)
        self.save()

    def delete_and_save(self, osm_id):
        if osm_id not in self.data:
            raise Exception ("Element not in set.")
        self.data.pop(osm_id)
        self.save()

    def save(self):
        # Pickle the current state of the data array to the file
        with open(self.filename, 'wb') as f:
            pickle.dump(self.data, f)
        self.notify_observers()

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

    def notify_observers(self):
        for observer in self._observers:
            observer.update(self)

