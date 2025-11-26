import argparse
from pathlib import Path

from grdb.devtools import make_dummy_database


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a test database")
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="Path where the test database will be created",
    )

    args = parser.parse_args()

    make_dummy_database(Path(args.path))


if __name__ == "__main__":
    main()
