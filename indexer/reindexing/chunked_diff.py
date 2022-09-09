import math

from tqdm import tqdm


def chunked_diff(diff_for_chunk, all_entries):
    def _chunks(big_list, chunk_length):
        for i in range(0, len(big_list), chunk_length):
            yield big_list[i : i + chunk_length]

    chunk_length = 500
    chunk_count = math.ceil(len(all_entries) / chunk_length)

    diff_list = []
    for chunk in tqdm(_chunks(all_entries, chunk_length), total=chunk_count):
        diff_list.append(diff_for_chunk(chunk))

    return [item for sublist in diff_list for item in sublist]
