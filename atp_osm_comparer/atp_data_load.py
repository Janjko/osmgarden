from collections import namedtuple

ATP_Set = namedtuple('ATP_Set', ['name', 'defining_tag' 'elements'])

def match_to_set(atp_set, obj, action, osm_set):
    brand_key = 'brand:wikidata'
    operator_key = 'operator:wikidata'
    ref_key = 'ref'
    matched_key = None
    if brand_key in obj.tags and obj.tags[brand_key] in atp_set:
        matched_key = brand_key
    elif operator_key in obj.tags and obj.tags[operator_key] in atp_set:
        matched_key = operator_key
    if matched_key is not None:
        
        if obj.tags[matched_key] not in atp_set:

            if action == 'm':                               # The osm element has wikidata tags, but they don't match with atp:
                if obj.id in osm_set.data:
                    osm_set.delete_and_save(obj.id)
            return
        
        if ref_key in obj.tags:
            matched_atp_name = find_name_by_element(obj.tags[ref_key], atp_set[obj.tags[matched_key]])

            if matched_atp_name is not None:                # The ref tag of the osm element matches the ref tag of an atp element:
                work_osm_element(atp_set, osm_set, action, obj.id, obj.tags['ref'], matched_atp_name)


            elif len(atp_set[obj.tags[matched_key]]) == 1:     # Ref tag exists, it doesn't match with any atp element, but there's only one atp set it can match:
                work_osm_element(atp_set, osm_set, action, obj.id, "", atp_set[obj.tags[matched_key]][0].name)
                
        
        elif len(atp_set[obj.tags[matched_key]]) == 1:         # There is no ref tag, but there's only one atp set it can match:
            work_osm_element(atp_set, osm_set, action, obj.id, "", atp_set[obj.tags[matched_key]][0].name)
    elif action == 'm':                                     # The osm element lost wikidata tags:
        if obj.id in osm_set.data:
            osm_set.delete_and_save(obj.id)

    
                    


def find_name_by_element(x, data):
    for item in data:
        if x in item.elements:
            return item.name
    return None

def get_defining_tag(obj_tags):
    if 'shop' in obj_tags:
        return {'shop':obj_tags['shop']}
    elif 'amenity' in obj_tags:
        return {'amenity':obj_tags['amenity']}
    else:
        return None

def work_osm_element(sets, osm_set, action, obj_id, obj_ref, matched_atp_name):
    if action == 'a':
        if obj_id in osm_set.data:
            raise Exception ("There's already an id of an element in our list that has just been added")
        osm_set.append_and_save(obj_id, matched_atp_name, obj_ref)
    elif action == 'd':
        if obj_id in osm_set.data:
            osm_set.delete_and_save(obj_id)
        else:
            raise Exception ("An element that should be in our set was deleted, but it isnt in our list.")
    elif action == 'm':
        if obj_id in osm_set.data:
            if osm_set.data[obj_id].name != matched_atp_name or osm_set.data[obj_id].element != obj_ref:
                osm_set.update_and_save(obj_id, matched_atp_name, obj_ref)
        else:
            osm_set.append_and_save(obj_id, matched_atp_name, obj_ref)