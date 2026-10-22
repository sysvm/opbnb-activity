def quantity(value):
    return int(value, 16) if isinstance(value, str) else int(value)


def resolve_range(get_block, latest_number, start_timestamp, end_timestamp):
    if start_timestamp >= end_timestamp:
        raise ValueError("start must precede end")
    cache = {}

    def timestamp(number):
        if number not in cache:
            block = get_block(number)
            if block is None:
                raise ValueError("historical block unavailable")
            cache[number] = quantity(block["timestamp"])
        return cache[number]

    if timestamp(0) > start_timestamp:
        raise ValueError("historical coverage does not include window start")
    if timestamp(latest_number) < end_timestamp:
        raise ValueError("incomplete window: head has not reached requested exclusive end")

    def lower_bound(target):
        lower, upper = 0, latest_number
        while lower < upper:
            middle = (lower + upper) // 2
            if timestamp(middle) < target:
                lower = middle + 1
            else:
                upper = middle
        return lower

    return lower_bound(start_timestamp), lower_bound(end_timestamp)
