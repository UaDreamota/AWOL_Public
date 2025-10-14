import argparse
import csv
import re
from pathlib import Path

def read_html_text(file_path):
    """
    Reads an .html file and returns the extracted HTML content as a string.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    return html_content

def parse_passage(text):
    """
    Returns a dictionary with the desired fields.
    If a field is not found, returns None (null) for that field.
    """

    fields = {
        "service_type": "N/A",
        "military_rank": None,
        "current_charges": None,
        "null-file": None,
        "misto-sudu": None,
        "occupation": None,
    }

    possible_ranks = [
        "солдат",
        "старший солдат",
        "молодший сержант",
        "сержант",
        "старший сержант",
        "головний сержант",
        "штаб-сержант",
        "старший майстер-сержант",
        "головний майстер-сержант",
        "молодший лейтенант",
        "лейтенант",
        "старший лейтенант",
        "капітан",
        "майор",
        "підполковник",
        "полковник",
        "бригадний генерал",
        "генерал-майор",
        "генерал-лейтенант",
        "генерал",
        "генерал-майор",
        "генерал-лейтенант",
        "генерал-полковник",
        "генерал армії України",
        "рекрут",
        "матрос",
        "старший сатрос",
        "старшина 2 статті",
        "старшина 1 статті",
        "головний старшина",
        "головний корабельний старшина",
        "штаб-старшина",
        "майстер-старшина",
        "старший майстер-старшина",
        "головний майстер старшина",
        "капітан-лейтенант",
        "капітан 3 рангу",
        "капітан 2 рангу",
        "капітан 1 рангу",
        "коммодор",
        "контр-адмірал",
        "віце-адмірал",
        "адмірал",
        "рядовий",
        "резерву",
        "запасу",
    ]

    # -- Service Type --------------------------------------------
    if "мобілізації" in text:
        fields["service_type"] = "Mobilisation"
    elif "контрактом" in text:
        fields["service_type"] = "Contract"
    elif "мобілізацієєю" in text:
        fields["service_type"] = "Mobilisation"

    # -- Military Rank -------------------------------------------
    for rank in possible_ranks:
        if rank.lower() in text.lower():
            fields["military_rank"] = rank
            break

    # -- Current Charges -----------------------------------------
    cc_match = re.search(r"передбаченого\s+ч\.\s*\d+\s+ст\.\s*(\d+)", text)
    if cc_match:
        fields["current_charges"] = cc_match.group(1).strip()

    # Null File Check
    if 'Інформація заборонена для оприлюднення згідно з пунктом чотири частини першої статті 7 Закону України "Про доступ до судових рішень"' in text:
        fields["null-file"] = "Yes"

    # Occupation
    cc_match = re.search(r"посаді\s+(\S+)", text)
    if cc_match:
        fields["occupation"] = cc_match.group(1).strip()

    return fields

def parse_args():
    repo_root = Path(__file__).resolve().parents[2]
    default_dir = repo_root / "data" / "awol_court"
    parser = argparse.ArgumentParser(description="Parse court HTML files into a CSV.")
    parser.add_argument(
        "--html-dir",
        type=Path,
        default=default_dir / "html",
        help="Directory containing HTML files.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=default_dir / "parsed_html_results.csv",
        help="Path to output CSV file.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # 1) Specify the folder containing the .html files
    folder_path = args.html_dir

    # 2) Find all .html files in that folder
    html_files = list(folder_path.glob("*.html"))

    # Prepare a list to store the results from each file
    all_results = []

    # 3) Loop over each file, read & parse
    for file_path in html_files:
        html_text = read_html_text(file_path)
        data = parse_passage(html_text)

        # You may also want to store which file this row came from:
        data["filename"] = file_path.name

        all_results.append(data)

    # 4) Write ALL results to a single CSV
    output_csv = args.output_csv

    # If no files were found or no data extracted, handle that case
    if not all_results:
        print("No data extracted or no .html files found!")
        return

    # The CSV columns should match the dictionary keys (including "filename")
    fieldnames = list(all_results[0].keys())

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)

    print(f"All done! {len(all_results)} files processed.")
    print(f"Results saved to: {output_csv}")

if __name__ == "__main__":
    main()
