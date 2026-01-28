#!/usr/bin/env python3
import argparse, json, sys
from urllib import request

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coord", required=True)
    ap.add_argument("cmd", choices=["set", "transfer", "status"])
    ap.add_argument("txid", nargs="?")
    ap.add_argument("args", nargs="*") # key/val or from/to/amt
    args = ap.parse_args()

    if args.cmd == "status":
        print(json.loads(request.urlopen(args.coord + "/status").read().decode()))
        return

    # Construct payload based on command 
    protocol = "2PC" # Default
    if args.cmd == "set":
        op = {"type": "SET", "key": args.args[0], "value": args.args[1]}
    elif args.cmd == "transfer":
        op = {"type": "TRANSFER", "from": args.args[0], "to": args.args[1], "amount": args.args[2]}

    payload = {"txid": args.txid, "protocol": protocol, "op": op}
    req = request.Request(args.coord + "/tx/start", data=json.dumps(payload).encode(), headers={"Content-Type":"application/json"})
    print(json.loads(request.urlopen(req).read().decode()))

if __name__ == "__main__":
    main()