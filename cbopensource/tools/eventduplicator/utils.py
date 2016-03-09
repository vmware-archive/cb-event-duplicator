from __future__ import absolute_import, division, print_function
import datetime
import json

__author__ = 'jgarman'


def split_process_id(guid):
    if type(guid) == int:
        return guid, 1

    return guid[:36], guid[37:]


def get_process_id(proc):
    old_style_id = proc.get('id', None)
    if old_style_id and old_style_id != '':
        return int(old_style_id)
    else:
        new_style_id = proc.get('unique_id', None)
        if not new_style_id:
            return None
        return new_style_id


def get_parent_process_id(proc):
    old_style_id = proc.get('parent_unique_id', None)
    if old_style_id and old_style_id != '':
        return old_style_id
    else:
        new_style_id = proc.get('parent_id', None)
        if not new_style_id:
            return None
        return int(new_style_id)


def json_encode(d):
    def default(o):
        if type(o) is datetime.date or type(o) is datetime.datetime:
            return o.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    return json.dumps(d, default=default)


def replace_sensor_in_guid(guid, new_id):
    # first eight characters of the GUID is the sensor ID
    return '%08x-%s' % (new_id, guid[9:])


def update_sensor_id_refs(proc, new_id):
    # this function will mutate proc in-place
    proc['sensor_id'] = new_id

    parent_unique_id = proc.get('parent_unique_id', None)
    if parent_unique_id:
        new_parent_id = replace_sensor_in_guid(parent_unique_id, new_id)
        proc['parent_unique_id'] = new_parent_id

    unique_id = proc.get('unique_id', None)
    if unique_id:
        new_unique_id = replace_sensor_in_guid(unique_id, new_id)
        proc['unique_id'] = new_unique_id

    return proc


def update_feed_id_refs(feed_data, new_id):
    # this function will mutate feed in-place
    feed_data['feed_id'] = new_id

    (_, doc_id) = feed_data['unique_id'].split(':')

    feed_data['unique_id'] = '%s:%s' % (new_id, doc_id)

    return feed_data
