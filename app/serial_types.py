def serial_type_from_int(n):
    """Return a tuple of (serial type, n_bytes) as described in '2.1. Record Format'"""
    match n:
        case 0:
            return ("NULL", 0)
        case 1 | 2 | 3 | 4 | 5 | 6:
            return ("int", n)
        case 7:
            return ("float", 8)
        case 8:
            return ("int", 0)
        case 9:
            return ("int", 1)
        case n if n >= 12 and n % 2 == 0:
            return ("blob", (n - 12) // 2)
        case n if n >= 13 and n % 2 == 1:
            return ("string", (n - 13) // 2)
        case _:
            raise ValueError(f"Unknown serial type specifier: {n}")


def read_serial(serial_type, data):
    """Decode data given the serial type and some bytes."""
    match serial_type:
        case ("NULL", 0):
            return "NULL"
        case ("string", n):
            return data[:n].decode("utf-8")
        case ("int", n):
            return int.from_bytes(data[:n], byteorder="big", signed=False)
        case _:
            raise ValueError(f"Unknown serial type: {serial_type}")
