import argparse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tg_harvest")
    subparsers = parser.add_subparsers(dest="command")

    web_parser = subparsers.add_parser("web", help="run the web application")
    web_parser.add_argument("--host", default="0.0.0.0")
    web_parser.add_argument("--port", type=int, default=8890)
    web_parser.add_argument("--debug", action="store_true")

    subparsers.add_parser("harvest", help="run the Telegram ingest job")

    args = parser.parse_args(argv)

    if args.command == "web":
        from tg_harvest.app.factory import run_web_server

        run_web_server(host=args.host, port=args.port, debug=args.debug)
        return 0

    if args.command == "harvest":
        from tg_harvest.ingest.runner import run_harvest

        run_harvest()
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
