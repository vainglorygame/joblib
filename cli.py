#!/usr/bin/python3

import os
import argparse
import asyncio

import joblib

async def main(args):
    queue = joblib.JobQueue()
    await queue.connect(host=args.host,
                        port=args.port,
                        user=args.user,
                        password=args.password,
                        database=args.database)
    await queue.setup()
    if args.cleanup:
        await queue.cleanup()

    if args.player:
        payload = {
            "region": args.region,
            "params": {
                "filter[createdAt-start]": "2017-02-01T00:00:00Z",
                "filter[playerNames]": args.player
            }
        }
        await queue.request(jobtype="grab",
                            payload=payload,
                            priority=args.priority)

parser = argparse.ArgumentParser(description="Request a Vainsocial update.")
parser.add_argument("-n", "--player",
                    help="Player name",
                    type=str)
parser.add_argument("-c", "--cleanup",
                    help="Reset all unfinished jobs",
                    action="store_true")
parser.add_argument("-r", "--region",
                    help="Specify a region",
                    type=str,
                    choices=["na", "eu", "sg"],
                    default="na")
parser.add_argument("-p", "--priority",
                    help="Set the priority",
                    type=int,
                    default=0)
parser.add_argument("--host",
                    help="Database host",
                    type=str,
                    default=os.environ.get("POSTGRESQL_HOST") or "localhost")
parser.add_argument("--port",
                    help="Database port",
                    type=int,
                    default=os.environ.get("POSTGRESQL_PORT") or 5432)
parser.add_argument("--user",
                    help="Database user",
                    type=str,
                    default=os.environ.get("POSTGRESQL_USER") or "vainsocial")
parser.add_argument("--password",
                    help="Database password",
                    type=str,
                    default=os.environ.get("POSTGRESQL_PASSWORD") or "")
parser.add_argument("--database",
                    help="Database name",
                    type=str,
                    default=os.environ.get("POSTGRESQL_DB") or "vainraw")
args = parser.parse_args()

loop = asyncio.get_event_loop()
loop.run_until_complete(main(args))
