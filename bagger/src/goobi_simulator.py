"""
Similar to migration tools, but accepts very dirty input,
or input that includes the same b number in different forms


deal with:
b12345678_0001
b12345678_0002 etc
"""
import sys
import requests
import time
import json


def main(filename):
    seen = set()
    print("[")
    with open(filename) as f:
        for line in f:
            b = line.strip().split("_")[0].lower()
            if b.startswith("b") and b not in seen:
                seen.add(b)
                r = requests.get("https://wellcomelibrary.org/goobipdf/" + b)
                try:
                    j = r.json()
                    print(json.dumps(j, indent=4))
                except json.decoder.JSONDecodeError:
                    t = r.text
                    msg = t[t.find('<title>') + 7:t.find('</title>')]
                    e = {
                        "identifier": b,
                        "ERROR": msg
                    }
                    print(json.dumps(e, indent=4))
                print(",")
                time.sleep(1)
    print("]")


if __name__ == "__main__":
    main(sys.argv[1])
