__author__ = 'jgarman'

import datetime
import json

def get_process_id(proc):
    old_style_id = proc.get('id', None)
    if old_style_id and old_style_id != '':
        return old_style_id
    else:
        new_style_id = proc.get('unique_id', None)
        if not new_style_id:
            log.warn("Process has no unique_id")
        return new_style_id

def get_parent_process_id(proc):
    old_style_id = proc.get('parent_id', None)
    if old_style_id and old_style_id != '':
        return old_style_id
    else:
        new_style_id = proc.get('parent_unique_id', None)
        if not new_style_id:
            log.warn("Process has no parent_unique_id")
        return new_style_id

def json_encode(d):
    def default(o):
        if type(o) is datetime.date or type(o) is datetime.datetime:
            return o.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    return json.dumps(d, default=default)
