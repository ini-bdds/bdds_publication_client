from globus_sdk import TransferClient
import json

class TransferBindingsClient(TransferClient):
    def __init__(self, **kwargs):
        TransferClient.__init__(self, **kwargs)

    def autoactivate_endpoint(self, endpoint_id):
        path = self.qjoin_path("endpoint", endpoint_id, "autoactivate")
        r = self.post(path)
        return r

    
    def endpoint_search(self, scope_filter = None, fulltext_filter=None, fields=None, offset=None, 
                        limit=None):
        params = {}
        if scope_filter:
            params['filter_scope'] = scope_filter
        if fulltext_filter:
            params['filter_fulltext'] = fulltext_filter
        if fields:
            params['fields'] = fields
        if offset:
            params['offset'] = offset
        if limit:
            params['limit'] = limit
            
        r = self.get("endpoint_search", params=params)
        return TransferBaseEntity(globus_response=r)

    def create_submissionid(self):
        r = self.get('submission_id')
        return TransferBaseEntity(globus_response=r)
        

class TransferBaseEntity():
    def __init__(self, json_data=None, globus_response=None, props=None, **kwargs):
        if json_data is not None:
            props = json.loads(json_data)
        if globus_response is not None:
            props = globus_response.data
        if props is None:
            props = kwargs

        if props is not None:
            for key, value in props.iteritems():
                if isinstance(value, dict):
                    value = TransferBaseEntity(props=value)
                try:
                    if not isinstance(value, basestring):
                        new_value = []
                        for val in value:
                            if isinstance(val, dict):
                                val = TransferBaseEntity(props=val)
                            new_value.append(val)
                        value = new_value
                except TypeError:
                    pass
                    
                setattr(self, key, value)

    def tojson(self):
# From http://stackoverflow.com/questions/3768895/python-how-to-make-a-class-json-serializable
        return json.dumps(self, default=lambda o: o.__dict__)

