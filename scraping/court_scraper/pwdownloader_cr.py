import argparse
from pwdownloader import download


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download criminal court decisions (wrapper around pwdownloader)."
    )
    parser.add_argument(
        "--output-csv",
        default="output.csv",
        help="Path to CSV file to write results",
    )
    args = parser.parse_args()
    download("criminal", args.output_csv)
