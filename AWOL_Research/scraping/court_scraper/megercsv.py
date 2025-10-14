import argparse
from pathlib import Path
import pandas as pd


def parse_args():
    repo_root = Path(__file__).resolve().parents[2]
    default_dir = repo_root / "data" / "awol_court"
    parser = argparse.ArgumentParser(description="Merge scraped CSV files.")
    parser.add_argument(
        "--csv1",
        type=Path,
        default=default_dir / "output12.csv",
        help="Path to the first CSV file.",
    )
    parser.add_argument(
        "--csv2",
        type=Path,
        default=default_dir / "parsed_html_results.csv",
        help="Path to the second CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_dir,
        help="Directory to save merged and unmatched CSV files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    output12 = pd.read_csv(args.csv1, encoding="utf-8")
    parsed_html_results = pd.read_csv(args.csv2, encoding="utf-8")

    first_column_name = output12.columns[0]

    output12[first_column_name] = (
        output12[first_column_name].astype(str).str.strip().str.lower()
    )
    parsed_html_results["filename"] = (
        parsed_html_results["filename"].astype(str).str.strip().str.lower()
    )

    parsed_html_results["filename_no_ext"] = parsed_html_results["filename"].str.replace(
        r"\.html$", "", regex=True
    )

    merged_df = pd.merge(
        output12,
        parsed_html_results,
        left_on=first_column_name,
        right_on="filename_no_ext",
        how="inner",
    )

    merged_df.drop(columns=["filename_no_ext"], inplace=True)

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    merged_df.to_csv(output_dir / "final_merged_output.csv", index=False)

    output12[~output12[first_column_name].isin(parsed_html_results["filename_no_ext"])].to_csv(
        output_dir / "unmatched_output12.csv", index=False
    )

    parsed_html_results[~parsed_html_results["filename_no_ext"].isin(output12[first_column_name])].to_csv(
        output_dir / "unmatched_parsed_html.csv", index=False
    )

    print(f"Merged file created: {output_dir / 'final_merged_output.csv'}")
    print(
        "Unmatched rows saved in:"
        f" {output_dir / 'unmatched_output12.csv'} and {output_dir / 'unmatched_parsed_html.csv'}"
    )


if __name__ == "__main__":
    main()

