import argparse


def main():
    parser = argparse.ArgumentParser(
        prog="flowr", description="Simple ETL written in python."
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"Flowr {__import__('flowr').__version__}",
        help="Show flowr version.",
    )

    subparsers = parser.add_subparsers(dest="command", required=False)

    # Run command.
    run_parser = subparsers.add_parser("run", help="Run the server (default).")

    # Setup command.
    setup_parser = subparsers.add_parser("setup", help="Setup the application.")

    # Execute the command.
    args = parser.parse_args()
    if args.command == "setup":
        from .setup import setup

        setup()

    else:
        from .run import run

        run()
