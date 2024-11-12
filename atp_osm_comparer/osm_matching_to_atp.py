from collections import namedtuple

ATP_Set = namedtuple('ATP_Set', ['name', 'defining_tag' 'elements'])

def match_to_set(atp_elements, obj, action, osm_set):
    brand_key = 'brand:wikidata'
    operator_key = 'operator:wikidata'
    matched_key = None
    if brand_key in obj.tags and obj.tags[brand_key] in atp_elements:
        matched_key = brand_key
    elif operator_key in obj.tags and obj.tags[operator_key] in atp_elements:
        matched_key = operator_key
    if matched_key is not None:
        
        if obj.tags[matched_key] not in atp_elements:

            if action == 'm':                               # The osm element has wikidata tags, but they don't match with atp:
                if obj.id in osm_set.data:
                    osm_set.delete_and_save(obj.id, obj.timestamp)
            return

        matched_atp_name, matched_ref = find_atp_name_and_ref_by_element(atp_elements[obj.tags[matched_key]], obj.tags)

        if matched_atp_name is not None:
            try:
                work_osm_element(atp_elements, osm_set, action, obj.id, matched_ref, matched_atp_name, obj.timestamp)
            except Exception as e:
                print(e)

    elif action == 'm':                                         # The osm element lost wikidata tags:
        if obj.id in osm_set.data:
            osm_set.delete_and_save(obj.id, obj.timestamp)

    
def find_atp_name_and_ref_by_element(spiders: list[ATP_Set], osm_tags):
    if 'ref' in osm_tags:
        osm_ref = osm_tags['ref']
    else:
        osm_ref = None
    matching_defining_tags=[]
    matching_ref=[]
    for spider in spiders:
        if all(key in osm_tags and osm_tags[key] == value for key, value in spider.defining_tag.items()):
            matching_defining_tags.append(spider.name)

    if len(matching_defining_tags) == 0:
        return None, None

    if osm_ref is not None:
        for spider in spiders:
            if osm_ref in spider.elements:
                matching_ref.append(spider.name)
        
    matching_both_ref_and_defining_tags = [name for name in matching_defining_tags if name in matching_ref]
    if len(matching_both_ref_and_defining_tags) == 1:
        return matching_both_ref_and_defining_tags[0], osm_ref
    elif len(matching_defining_tags) == 1:
        return matching_defining_tags[0], None
    elif len(matching_ref) == 1:
        return matching_ref[0], osm_ref
    else:
        return None, None

    

def work_osm_element(atp_elements, osm_set, action, obj_id, obj_ref, matched_atp_name, timestamp):
    if action == 'a':
        if obj_id in osm_set.data:
            raise Exception ("There's already an id of an element in our list that has just been added")
        osm_set.append_and_save(obj_id, matched_atp_name, obj_ref, timestamp)
    elif action == 'd':
        if obj_id in osm_set.data:
            osm_set.delete_and_save(obj_id, timestamp)
        else:
            raise Exception ("An element that should be in our set was deleted, but it isnt in our list.")
    elif action == 'm':
        if obj_id in osm_set.data:
            if osm_set.data[obj_id].name != matched_atp_name or osm_set.data[obj_id].element != obj_ref:
                osm_set.update_and_save(obj_id, matched_atp_name, obj_ref, timestamp)
        else:
            osm_set.append_and_save(obj_id, matched_atp_name, obj_ref, timestamp)

