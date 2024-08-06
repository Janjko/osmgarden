import json
from lxml import etree
import os
import zipfile
from pathlib import Path

def create_new_import_xmls(zip_file_path):
    directory_path = './import_xml_templates'
    xsd_file = 'osm-import-schema/intent.xsd'
    with open(xsd_file) as f:
        xmlschema_doc = etree.parse(f)
        xmlschema = etree.XMLSchema(xmlschema_doc)
    
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    for filename in os.listdir(directory_path):
        name=''
        if filename.endswith('.xml'):
            name = filename.rstrip('.xml')
            full_path = os.path.join(directory_path, filename)
            xml_doc = etree.parse(full_path)
            if xmlschema.validate(xml_doc):
                template_elements = xml_doc.xpath('//template[@type="alltheplaces" and @spider]')
                if template_elements:
                    spider = template_elements[0].get('spider')
                    atp_object = get_atp_object(zip_file_path, spider)
                    new_osm_xml = fill_template(template_elements[0], atp_object)
                    for child in new_osm_xml.xpath('/osm/child::*'):
                        template_elements[0].getparent().append(child)
                    template_elements[0].getparent().remove(template_elements[0])
        Path("./import_xml_generated").mkdir(parents=True, exist_ok=True)
        xml_doc.write(f"import_xml_generated/{name}.xml", pretty_print=True, xml_declaration=True, encoding="UTF-8")

    
def get_atp_object(zip_file_path, spider):
    data = None
    target_file_name = "output/"+spider+".geojson"
    with zipfile.ZipFile(zip_file_path, "r") as z:
        # Check if the target file exists in the zip archive
        if target_file_name in z.namelist():
            # Read the content of the target file
            with z.open(target_file_name) as f:
                data = f.read()
        else:
            print(f"File '{target_file_name}' not found in the zip archive.")
    return json.loads(data.decode("utf-8"))

def fill_template(template_xml, atp_object):
    tags = template_xml.xpath('/osm/template[@type="alltheplaces" and @spider]//tag[@template="yes"]/@k')
    matchtags = template_xml.xpath('/osm/template[@type="alltheplaces" and @spider]//tag[@template="yes" and @function="match"]/@k')
    first_child_tag = template_xml.xpath("//template/*[1]")[0]
    osm_xml = etree.Element("osm")
    for atp_entry in atp_object['features']:
        
        # Create a new XML object with the same tag
        child_xml = etree.Element(first_child_tag.tag, first_child_tag.attrib)
        child_xml.set("lat", str(atp_entry['geometry']['coordinates'][1]))
        child_xml.set("lon", str(atp_entry['geometry']['coordinates'][0]))
        child_xml.set("atp-id", atp_entry['id'])
        child_xml.set("atp-spider", atp_entry['properties']['@spider'])
        for key, value in atp_entry['properties'].items():
            if key in tags:
                tag_xml = etree.Element("tag")
                tag_xml.set("k", key)
                tag_xml.set("v", value)
                if key in matchtags:
                    tag_xml.set("function", "match")
                child_xml.append(tag_xml)
        osm_xml.append(child_xml)
    return osm_xml

create_new_import_xmls("./output.zip")