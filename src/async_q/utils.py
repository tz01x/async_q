import msgpack


def serialize(value):
    return msgpack.packb(value)

def deserialize(byte_value):
    return msgpack.unpackb(byte_value)