def read_varint(data):
    """Decode a single varint from bytes and return it and the rest of the data."""
    result = 0
    shift = 0

    for i, byte in enumerate(data):
        if i == 8:
            break
        result = (result << shift) | (byte & 0x7F)
        shift += 7
        if not byte & 0x80:
            return result, data[i + 1 :]
    # Not tested!
    result = (result << 8) | byte
    return result, data[8:]
