#!/usr/bin/python

from datetime import datetime, tzinfo
import os
import shutil
import argparse
import json
from feedgen.feed import FeedGenerator
from enum import Enum
from xmldiff import main, actions
from lxml import etree
import hashlib
import base64

class change_status(str, Enum):
    CREATE_NONE = 'CREATE_NONE'
    MODIFY_NONE = 'MODIFY_NONE'
    CREATE_MODIFY = 'CREATE_MODIFY'
    MODIFY_CREATE = 'MODIFY_CREATE'
    NONE_CREATE = 'NONE_CREATE'
    NONE_MODIFY = 'NONE_MODIFY'


CHANGE_STATUS = 'change_status'
ELEMENT_REF = 'element_ref'
OSM_ID = 'osm_id'

RSS_RAW_FILENAME = "rss_raw.json"
#RSS_FILENAME = "rss.xml"

fg = FeedGenerator()
fg.id('http://lernfunk.de/media/654321')
fg.title('Some Testfeed')
fg.author({'name': 'John Doe', 'email': 'john@example.de'})
fg.link(href='http://example.com', rel='alternate')
fg.logo('http://ex.com/logo.jpg')
fg.subtitle('This is a cool feed!')
fg.link(href='http://larskiesow.de/test.atom', rel='self')
fg.language('en')

def compute_hash(tag_values) -> str:
    sha1 = hashlib.sha1()
    for tag in tag_values:
        sha1.update(tag.encode("utf8"))
    return base64.urlsafe_b64encode(sha1.digest()).decode("utf8")

def add_ids(xml_doc):
    for element in xml_doc.xpath('/osm/child::*'):
        if element.tag == "domain":
            continue
        matching_tag_values = sorted(element.xpath("./tag[@function='match']/@v"))
        element.attrib['matchId'] = compute_hash(matching_tag_values)


# Dictionary that holds the raw rss data, from which the rss is created
rss_raw = []

TITLE = "OSM Garden"
parser = argparse.ArgumentParser(
    description='''[].
        Reads a profile with source data and conflates it with OpenStreetMap data.
        Produces an JOSM XML file ready to be uploaded.'''.format(TITLE))

parser.add_argument('-n', '--new', type=argparse.FileType('r',
                    encoding='utf-8'), help='New file')
parser.add_argument('-i', '--inspected', type=argparse.FileType('r',
                    encoding='utf-8'), help='Output OSM XML file name')
parser.add_argument(
    '-r', '--rss', type=argparse.FileType('w'), help='RSS XML file')

options = parser.parse_args(["-n", "C:\\Users\\janko\\source\\osmgarden\\compare_results\\2024-08-01T11_38_12Z.xml", "-i",
                           "C:\\Users\\janko\\source\\osmgarden\\compare_results\\2024-08-01T10_59_31Z.xml", "-r", "C:\\Users\\janko\\source\\osmgarden\\rss\\rss.xml"])

#options = parser.parse_args()

try:
    with open(RSS_RAW_FILENAME, 'r') as json_file:
        rss_raw = json.load(json_file)
except IOError:
    with open(RSS_RAW_FILENAME, 'w') as json_file:
        json.dump(rss_raw, json_file)

oldXml = etree.parse(options.inspected)
newXml = etree.parse(options.new)

add_ids(oldXml)
add_ids(newXml)

diffs = main.diff_trees(oldXml, newXml,
                 diff_options={'F': 0.5, 'ratio_mode': 'fast', 'uniqueattrs':['matchId']})

# XMLs should be normalized so that all empty matches tags are deleted

for diff in diffs:
    if isinstance(diff, actions.InsertAttrib) and diff.name == 'id' and  newXml.xpath(diff.node)[0].getparent().tag == 'matches' and newXml.xpath(diff.node)[0].tag in ['node', 'way', 'relation']:
        rss_raw.append({CHANGE_STATUS: change_status.CREATE_NONE,
                                ELEMENT_REF: newXml.xpath(diff.node)[0].getparent().getparent().xpath('./tag[@k="ref"]/@v')})

with open(RSS_RAW_FILENAME, 'w') as fp:
    json.dump(rss_raw, fp)

for entry in rss_raw:
    fe = fg.add_entry()
    if entry[CHANGE_STATUS] == change_status.CREATE_NONE or entry[CHANGE_STATUS] == change_status.MODIFY_NONE:
        fe.title("Element ispravno ucrtan.")
        fe.description('Ispravno ucrtan element ' + str(entry[ELEMENT_REF]))
    if entry[CHANGE_STATUS] == change_status.NONE_CREATE or entry[CHANGE_STATUS] == change_status.MODIFY_CREATE:
        fe.title("Element obrisan!")
        fe.description('Element ' + str(entry[ELEMENT_REF]) + ' obrisan, ili je izgubio osnovne tagove.')
    if entry[CHANGE_STATUS] == change_status.NONE_MODIFY:
        fe.title("Elementu pokvareni tagovi.")
        fe.description('Elementu ' + str(entry[ELEMENT_REF]) + ' pokvareni tagovi.')
    if entry[CHANGE_STATUS] == change_status.CREATE_MODIFY:
        fe.title("Element ucrtan, ali sa lošim tagovima.")
        fe.description('Element ' + str(entry[ELEMENT_REF]) + ' ucrtan, ali sa lošim tagovima.')

fg.rss_str(pretty=True)
fg.rss_file(options.rss.name)