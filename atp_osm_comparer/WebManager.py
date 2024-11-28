from autosavearray import AutoSaveArray
from collections import defaultdict
import os
from git import Repo
import json

class WebManager:

    def __init__(self, data_folder, atp_set, nodes: AutoSaveArray, ways: AutoSaveArray, relations: AutoSaveArray):
        self.nodes = nodes
        self.ways = ways
        self.relations = relations
        self.nodes.register_observer(self)
        self.ways.register_observer(self)
        self.relations.register_observer(self)
        self.data_folder = data_folder
        self.html_filename = os.path.join(self.data_folder, "index.html")
        self.json_filename = os.path.join(self.data_folder, "data.json")
        self.atp_set = atp_set
        self.seqid = None

    def update(self, timestamp):

        grouped_data = self.get_state()
        
       # Create an HTML table
        html_table = f"""
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.5/css/jquery.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script type="text/javascript" src="https://cdn.datatables.net/1.13.5/js/jquery.dataTables.min.js"></script>
        <span id="seqid">Sequence ID: {self.seqid}</span>
        <table id="myTable" class="display">
            <thead>
                <tr>
                    <th>Spider name</th>
                    <th>OSM objects matching a spider</th>
                    <th>OSM objects matching an element</th>
                    <th>Objects in ATP set</th>
                    <th>Defining tag</th>
                    <th>Wikidata id</th>
                    <th>Last changed</th>
                </tr>
            </thead>
            <tbody>
        """

        # Add rows to the table based on the grouped data
        for name, counts in grouped_data.items():
            html_table += f"""
            <tr>
                <td>{name}</td>
                <td>{counts['total']}</td>
                <td>{counts['non_none']}</td>
                <td>{counts.get('atp_total', 'N/A')}</td>
                <td>{counts.get('defining_tag')}</td>
                <td>{counts.get('wikidata')}</td>
                <td>{counts.get('timestamp', 0)}</td>
            </tr>
            """

        html_table += """
            </tbody>
        </table>
        <script>
            $(document).ready(function() {
                $('#myTable').DataTable();
            });
        </script>
        """

        # Output the HTML table
        with open(os.path.join(self.data_folder, "config.json")) as f:
            config = json.load(f)
        self.save_to_git(config["github_username"], config["github_token"], config["github_repo_link"], html_table, timestamp.strftime("%m/%d/%Y, %H:%M:%S") )

    def save_to_git(self, github_username, github_token, github_repo_link, html, commit_message):
        repo_folder = os.path.join(self.data_folder, github_repo_link.split('/')[-1])
        github_repo_auth = github_repo_link.replace('https://', 'https://'+ github_username + ':' + github_token + '@')
        if not os.path.exists(repo_folder):
            Repo.clone_from(github_repo_auth, repo_folder)
        repo = Repo(repo_folder)
        git_file_path = 'index.html'
        file_path = os.path.join(repo_folder, git_file_path)
        with open(file_path, 'w') as f:
            f.write(html)
        repo.index.add([git_file_path])
        repo.index.commit(commit_message)
        repo.remotes.origin.push(refspec='main:main')

    def get_state(self):
        objects = [self.nodes, self.ways, self.relations]
        grouped_data = defaultdict(lambda: {'total': 0, 'non_none': 0})
        for obj in objects:
            for key, value in obj.data.items():
                grouped_data[value.name]['total'] += 1
                if 'timestamp' not in grouped_data[value.name]:
                    grouped_data[value.name]['timestamp'] = str(value.timestamp)
                else:
                    grouped_data[value.name]['timestamp'] = max(grouped_data[value.name]['timestamp'], str(value.timestamp))
                if value.element is not None:
                    grouped_data[value.name]['non_none'] += 1

        for wikiid, atp_sets in self.atp_set.items():
            for atp_set in atp_sets:
                entry = grouped_data[atp_set.name]
                entry['atp_total'] = len(atp_set.refs)
                entry['defining_tag'] = atp_set.defining_tag
                entry['wikidata'] = wikiid
        return grouped_data
    
    def update_json(self):

        grouped_data = self.get_state()
        json.dump(grouped_data, open(self.json_filename, 'w'))