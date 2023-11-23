import jsonschema

put_schema = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "title": "PUT request",
    "description": "A client has sent a request for a job to be added",
    "properties": {
        "request": {"type": "string"},
        "queue": {"type": "string"},
        "job": {"type": "object"},
        "pri": {"type": "integer", "minimum": 0},
    },
    "required": ["request", "queue", "job", "pri"],
    "additionalProperties": False,
}

get_schema = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "title": "GET request",
    "description": "A client has requested a job from the specified queues",
    "properties": {
        "request": {"type": "string"},
        "queues": {"type": "array", "items": {"type": "string"}, "minItems": 1},
        "wait": {"type": "boolean"},
    },
    "required": ["request", "queues"],
    "additionalProperties": False,
}

abort_delete_schema = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "title": "ABORT/DELETE request",
    "description": "A client has requested a job to be removed or aborted",
    "properties":{
        "request": {"type": "string", "enum": ["abort", "delete"]},
        "id":{"type":"number", "minimum": 0}
    },
    "required": ["request", "id"],
    "additionalProperties": False,
}

instance = {
    "request": "put",
    "queue": "queue1",
    "job": {"title": "example-job"},
    "pri": 123,
}
instance2={"request":"get","queues":["queue1"], "wait": True}
instance3 = {"request":"delete","id":12345}

jsonschema.validate(instance=instance, schema=put_schema)
jsonschema.validate(instance=instance2, schema=get_schema)
jsonschema.validate(instance=instance3, schema=abort_delete_schema)
