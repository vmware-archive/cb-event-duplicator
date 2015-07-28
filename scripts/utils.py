__author__ = 'jgarman'

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
