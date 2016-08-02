import json
import msgpack
from collections import namedtuple

codec = namedtuple('codec', 'encode decode')


def encode_msgpack(v):
    return msgpack.packb(v)


def decode_msgpack(v):
    return msgpack.unpackb(v, use_list=False)


def encode_json(v):
    return json.dumps(v)


def decode_json(v):
    return json.loads(v)


codecs = {
    'application/json': codec(encode_json, decode_json),
    'application/msgpack': codec(encode_msgpack, decode_msgpack),
}
