from autosavearray import AutoSaveArray
from collections import defaultdict
import os

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
        self.atp_set = atp_set

    def update(self, b):
        objects = [self.nodes, self.ways, self.relations]
        grouped_data = defaultdict(lambda: {'total': 0, 'non_none': 0})
        for obj in objects:
            for key, value in obj.data.items():
                grouped_data[value.name]['total'] += 1
                if value.element is not None:
                    grouped_data[value.name]['non_none'] += 1

        for wikiid, atp_sets in self.atp_set.items():
            for atp_set in atp_sets:
                if atp_set.name in grouped_data:
                    grouped_data[atp_set.name]['atp_total'] = len(atp_set.elements)
                else:
                    grouped_data[atp_set.name]['total'] = 0
                    grouped_data[atp_set.name]['non_none'] = 0
                    grouped_data[atp_set.name]['atp_total'] = len(atp_set.elements)
        # Create an HTML table
        html_table = """
        <table border="1">
            <thead>
                <tr>
                    <th>Spider name</th>
                    <th>OSM objects matching an element in spider</th>
                    <th>OSM objects matching spider, not matching an element</th>
                    <th>Objects in ATP set</th>
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
            </tr>
            """

        html_table += """
            </tbody>
        </table>
        """

        # Output the HTML table
        with open(self.html_filename, 'w') as f:
            f.write(html_table)