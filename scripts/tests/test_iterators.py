from helpers.iterators import chunked_iterable


def test_chunked_iterable():
    assert list(chunked_iterable(range(10), size=3)) == [
        (0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)
    ]

    assert list(chunked_iterable(range(10), size=5)) == [
        (0, 1, 2, 3, 4), (5, 6, 7, 8, 9,)
    ]
