import itertools


def chunked_iterable(iterable, size):
    """
    See https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk
