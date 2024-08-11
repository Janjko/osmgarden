#!/usr/bin/python

from datetime import datetime, tzinfo
import os
import shutil
import argparse
import json
from feedgen.feed import FeedGenerator
from lxml import etree
import hashlib
import base64
from pathlib import Path

CREATE_NONE = 'CREATE_NONE'
MODIFY_NONE = 'MODIFY_NONE'
CREATE_MODIFY = 'CREATE_MODIFY'
MODIFY_CREATE = 'MODIFY_CREATE'
NONE_CREATE = 'NONE_CREATE'
NONE_MODIFY = 'NONE_MODIFY'


CHANGE_STATUS = 'change_status'
ELEMENT_LINK = 'element_link'
OSM_ID = 'osm_id'
NEW_TIMESTAMP = 'new_osm_timestamp'
OLD_TIMESTAMP = 'old_osm_timestamp'
EVENTS = 'events'

RSS_RAW_FILENAME = "rss_raw.json"
#RSS_FILENAME = "rss.xml"

fg = FeedGenerator()
fg.id('http://lernfunk.de/media/654321')
fg.title('konzum_hr')
fg.author({'name': 'Janko Mihelić', 'email': 'john@example.de'})
fg.link(href='https://osm.org', rel='alternate')
fg.logo('https://adnet.hr/wp-content/themes/vdtheme/images/logo.png')
fg.subtitle('This is a cool feed!')
fg.link(href='https://adnet.hr/konzum_hr.xml', rel='self')
fg.language('en')

def compute_hash(tag_values) -> str:
    if len(tag_values) == 1:
        return tag_values[0]
    sha1 = hashlib.sha1()
    for tag in tag_values:
        sha1.update(tag.encode("utf8"))
    return base64.urlsafe_b64encode(sha1.digest()).decode("utf8")

def add_ids(xml_doc):
    for element in xml_doc.xpath('/osm/child::*'):
        if element.tag == "domain":
            continue
        matching_tag_values = sorted(element.xpath("./tag[@function='match']/@v"))
        matching_tag_hash = compute_hash(matching_tag_values)
        element.attrib['id'] = matching_tag_hash + "_DIVIDE_element"
        matches_element = element.find('id')
        if matches_element != None:
            matches_element.attrib['id'] = matching_tag_hash + "_DIVIDE_match"
        for tag in element.findall('tag'):
            tag.attrib['id'] = matching_tag_hash + "_DIVIDE_" + tag.attrib['k']


# Dictionary that holds the raw rss data, from which the rss is created
rss_raw = []

TITLE = "OSM Garden"
parser = argparse.ArgumentParser(
    description='''[].
        Reads a profile with source data and conflates it with OpenStreetMap data.
        Produces an JOSM XML file ready to be uploaded.'''.format(TITLE))

parser.add_argument(
    '-r', '--rss', type=argparse.FileType('w'), help='RSS XML file')

options = parser.parse_args(["-r", "C:\\Users\\janko\\source\\osmgarden\\rss\\rss.xml"])
directory_path = 'compare_results/'
rss_directory_path = 'rss/'
projectName = 'konzum_hr'

reference_date = datetime(1, 1, 1)

try:
    with open(RSS_RAW_FILENAME, 'r') as json_file:
        rss_raw = json.load(json_file)
except IOError:
    with open(RSS_RAW_FILENAME, 'w') as json_file:
        json.dump(rss_raw, json_file)

while True:

    if len(rss_raw)>0:
        reference_date = datetime.strptime(rss_raw[-1]['new_osm_timestamp'], "%Y-%m-%dT%H:%M:%SZ")

    xml_files = [filename for filename in os.listdir(directory_path) if filename.startswith(projectName+"@") and filename.endswith(".xml")]

    dates = []
    for xml_file in xml_files:
        date_str = xml_file.split("@")[1].split(".xml")[0]
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H_%M_%SZ")
        dates.append(date_obj)

    try:
        closest_newer_date = min(filter(lambda d: d >= reference_date, dates))
        next_date = min(filter(lambda d: d > closest_newer_date, dates))
    except:
        break



    closest_newer_date_string = directory_path + f'konzum_hr@{closest_newer_date.strftime("%Y-%m-%dT%H_%M_%SZ")}.xml'
    next_date_string = directory_path + f'konzum_hr@{next_date.strftime("%Y-%m-%dT%H_%M_%SZ")}.xml'
    oldXml = etree.parse(closest_newer_date_string)
    newXml = etree.parse(next_date_string)

    add_ids(oldXml)
    add_ids(newXml)

    #oldXml.write(f"test/old.xml", pretty_print=True, xml_declaration=True, encoding="UTF-8")
    #newXml.write(f"test/new.xml", pretty_print=True, xml_declaration=True, encoding="UTF-8")

    rss_raw_element = {NEW_TIMESTAMP: newXml.getroot().attrib['timestamp_osm_base'],
                    OLD_TIMESTAMP: oldXml.getroot().attrib['timestamp_osm_base'],
                    EVENTS: []}

    for old_element in oldXml.xpath('/osm/child::*'):
        if old_element.tag == "domain":
            continue
        for new_element in newXml.xpath('/osm/child::*'):
            if new_element.tag == "domain":
                continue
            if old_element.attrib['id'] == new_element.attrib['id']:
                old_matches = old_element.findall('matches/')
                new_matches = new_element.findall('matches/')
                old_match_no = len(old_matches)
                new_match_no = len(new_matches)
                if old_match_no == new_match_no == 1:
                    continue
                if old_match_no == new_match_no == 0:
                    continue
                if new_match_no < old_match_no == 1:
                    element_type = old_matches[0].tag
                    element_id = old_matches[0].attrib['id']
                    rss_raw_element[EVENTS].append({CHANGE_STATUS: NONE_CREATE,
                                                    ELEMENT_LINK: f'https://osm.org/{element_type}/{element_id}'})


    rss_raw.append(rss_raw_element)

    with open(RSS_RAW_FILENAME, 'w') as fp:
        json.dump(rss_raw, fp)

for rss_entry in rss_raw:
    fe = fg.add_entry()
    fe.title(f"Elementi obrisani ili pokvareni ({rss_entry[NEW_TIMESTAMP]})")
    description = []
    for entry in rss_entry[EVENTS]:
        if entry[CHANGE_STATUS] == NONE_CREATE or entry[CHANGE_STATUS] == MODIFY_CREATE:
            description.append(f'<a href="{str(entry[ELEMENT_LINK])}">Element</a> obrisan, ili je izgubio osnovne tagove.')
        if entry[CHANGE_STATUS] == NONE_MODIFY:
            description.append(f'<a href="{str(entry[ELEMENT_LINK])}">Elementu</a> pokvareni tagovi.')
        fe.description('\n'.join(description))

fg.rss_file(rss_directory_path + projectName + '.xml')