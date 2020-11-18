import itertools


def chunked_iterable(iterable, *, size):
    """
    Given an iterable, divide it into chunks of fixed size.

    >>> chunked_iterable(range(10), size=3)
    (0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)

    See https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/

    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk
