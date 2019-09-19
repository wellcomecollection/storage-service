#!/usr/bin/env python
# -*- encoding: utf-8

from dynamodb import scan_dynamodb_table


if __name__ == "__main__":
    out_dir = scan_dynamodb_table("storage-ingests", max_workers=20)

    print(out_dir)
