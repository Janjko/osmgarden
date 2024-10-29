import requests
import json
import os
import zipfile
import shutil
from collections import namedtuple
from collections import defaultdict
import pickle
import atp_data_load

ATP_Set = namedtuple('ATP_Set', ['name', 'defining_tag', 'elements'])

class ATPDataManager:
    def __init__(self, data_folder):
        self.atp_history_url = "https://data.alltheplaces.xyz/runs/history.json"
        self.data_folder = data_folder
        self.unziped_data_foldername = os.path.join(self.data_folder, "output") # ATP zip unzips to this folder automaticaly
        self.atp_download_filename = os.path.join(self.data_folder, "atp_file.zip")
        self.atp_pickeled_set_prefix = "atp_sets_"
        self.atp_pickeled_set_extension = ".bin"
        self.sets = {}
        self.atp_pickeled_set_file, self.current_atp_run_id = self.get_file_by_prefix(self.data_folder, self.atp_pickeled_set_prefix)

    def delete_temp_files(self):
        if os.path.exists(self.unziped_data_foldername):
            shutil.rmtree(self.unziped_data_foldername)
        if os.path.exists(self.atp_download_filename):
            os.remove(self.atp_download_filename)

    def get_history_json(self):
        response = requests.get(self.atp_history_url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data. Status code: {response.status_code}")
            return None

    def download_atp_file(self, url, local_filename):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True


    def download_atp(self):
        new_data = False
        history_data = self.get_history_json()
        if history_data:
            last_run_id = history_data[-1]["run_id"]
            output_zip_url = history_data[-1]["output_url"]

            if last_run_id != self.current_atp_run_id:
                if not os.path.exists(self.atp_download_filename):
                    successDownload = self.download_atp_file(output_zip_url, self.atp_download_filename)
                successUnzip = self.unzip_atp(self.atp_download_filename)
                self.current_atp_run_id = last_run_id
                os.remove(self.atp_download_filename)
                self.sets = self.get_atp_sets()

                new_data = True
        self.manage_pickles(new_data)
        self.delete_temp_files()

    def manage_pickles(self, new_data_downloaded):
        if new_data_downloaded:
            self.atp_pickeled_set_file = os.path.join(self.data_folder, self.atp_pickeled_set_prefix + self.current_atp_run_id + self.atp_pickeled_set_extension)
            if os.path.exists(self.atp_pickeled_set_file):
                shutil.rmtree(self.atp_pickeled_set_file)
            with open(self.atp_pickeled_set_file, "wb") as f:
                pickle.dump(self.sets, f)
        else:
            if not os.path.exists(self.atp_pickeled_set_file):
                print("Error. No new set, and no saved set.")
            else:
                with open(self.atp_pickeled_set_file, "rb") as f:
                    self.sets = pickle.load(f)

    def unzip_atp(self, zip_file):
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(self.data_folder)
        return True
    
    def get_file_by_prefix(self, directory, prefix):
        for file_name in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, file_name)) and file_name.startswith(prefix):
                # Get the rest of the name after the prefix
                rest_of_name = file_name[len(prefix):].strip(self.atp_pickeled_set_extension)
                return os.path.join(directory, file_name), rest_of_name
        return None, None
    
    def get_atp_sets(self):
        """Takes ATP geojson files from path and returns an array of ATP_Set named tuples. 

        Goes over all the elements of each geojson, finds if all elements have brand:wikidata or operator:wikidata, and if so, creates a new element in the sets array.
        
        If all elements don't have one of those tags, it skips.
        """
        sets = {}
        path = self.unziped_data_foldername
        for filename in os.listdir(path):
            #if str.startswith(filename, 'b'):
            #    break
            with open(os.path.join(path, filename), "r") as f:
                try:
                    atp_object = json.load(f)
                except:
                    print (f"File {filename} invalid")
                    continue
                name = atp_object['dataset_attributes']['@spider']
                common_tags = {}
                temp_atm_items = []
                for feature in atp_object["features"]:
                    if 'ref' not in feature['properties']:
                        continue
                    properties = {key:value
                                for key, value in feature['properties'].items()
                                if key == 'brand:wikidata' or
                                (key=='operator:wikidata' and 'brand:wikidata' not in feature['properties'])}
                    if not common_tags:
                        # Initialize common_properties with the first feature's properties
                        common_tags = properties
                    else:
                        # Update common_properties with common key-value pairs
                        common_tags = {
                            key: value
                            for key, value in properties.items()
                            if key in common_tags and value == common_tags[key]
                        }
                    temp_atm_items.append(feature['properties']['ref'])

                    
                    defining_tag = atp_data_load.get_defining_tag(feature['properties'])
                    if defining_tag == None:
                        continue
                if (len(common_tags)==1):
                    wikidata_value = common_tags[next(iter(common_tags))]
                    if wikidata_value in sets:
                        sets[wikidata_value].append(ATP_Set(name, defining_tag, temp_atm_items))
                    else:
                        sets[wikidata_value] = [ATP_Set(name, defining_tag, temp_atm_items)]
                print("Common key-value pairs:", common_tags)
        return sets
