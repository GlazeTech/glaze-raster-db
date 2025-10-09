import argparse
import sys
from pathlib import Path

# Add parent directory to path so we can import from tests module
sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.mock import make_dummy_database


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
