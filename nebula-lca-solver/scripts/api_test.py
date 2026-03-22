import argparse
import json
from pathlib import Path
from urllib import request


def main() -> None:
    parser = argparse.ArgumentParser(description="Post snapshot payload to /v1/lcia.")
    parser.add_argument("--snapshot", required=True, help="Path to snapshot JSON.")
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000/v1/lcia",
        help="API endpoint URL.",
    )
    args = parser.parse_args()

    snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
    payload = json.dumps({"snapshot": snapshot}).encode("utf-8")

    req = request.Request(
        args.url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        print(body)


if __name__ == "__main__":
    main()
