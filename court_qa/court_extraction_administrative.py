# -*- coding: utf-8 -*-
import os
import re
import csv
from pathlib import Path
from typing import Tuple, Dict, Any, List

from bs4 import BeautifulSoup
from openai import OpenAI

# ---------------- CONFIG ----------------
INPUT_DIR = r"D:\Projects\GitRepositories\TheAWOLthing\AWOL_Research\data\awol_court\decisions_adm"  # <- set your folder
OUTPUT_CSV = r"D:\Projects\GitRepositories\TheAWOLthing\AWOL_Research\court_qa\administrative_gpt5.csv"
MAX_FILES = 25000
MODEL = "gpt-5-nano"        # For higher accuracy, consider: "gpt-4o-mini"
MAX_TEXT_CHARS = 120_000
TIMEOUT_S = 60
# ---------------------------------------

client = OpenAI()
HEADER_TAG = re.compile(r"^h[1-3]$", re.I)

# === Fields to extract (API-only) ===
FIELDS: List[Dict[str, Any]] = [
    # Подія СЗЧ / AWOL
    {
        "name": "awol_start_date_iso",
        "type": "string",
        "description": (
            "Дата ПОЧАТКУ самовільного залишення місця/військової частини у форматі YYYY-MM-DD. "
            "Якщо невідомо — порожній рядок."
        ),
        "required": False
    },
    {
        "name": "awol_start_time_hhmm",
        "type": "string",
        "description": (
            "Час ПОЧАТКУ СЗЧ у форматі HH:MM (24-годинний). "
            "Якщо згадано приблизно (напр., 'приблизно о 8:30'), все одно нормалізуй до HH:MM. "
            "Якщо невідомо — порожній рядок."
        ),
        "required": False
    },
    {
        "name": "return_date_iso",
        "type": "string",
        "description": (
            "Дата ПОВЕРНЕННЯ (або затримання) до місця служби у форматі YYYY-MM-DD. "
            "Якщо невідомо — порожній рядок."
        ),
        "required": False
    },
    {
        "name": "return_time_hhmm",
        "type": "string",
        "description": (
            "Час ПОВЕРНЕННЯ (або затримання) у форматі HH:MM (24-годинний). "
            "Якщо невідомо — порожній рядок."
        ),
        "required": False
    },
]
# =====================================

def load_text_excerpt(path: Path) -> str:
    """
    Parse HTML and build a concise text excerpt prioritizing
    title, headers, then body.
    """
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    parts = []
    if soup.title and soup.title.text:
        parts.append(soup.title.text.strip())

    for tag in soup.find_all(HEADER_TAG):
        t = tag.get_text(" ", strip=True)
        if t:
            parts.append(t)

    main = soup.find("main")
    parts.append((main or soup).get_text(" ", strip=True))
    text = "\n".join(parts)
    return text[:MAX_TEXT_CHARS]

def _cleanup_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = re.sub(r"\s+", " ", s).strip(" ,.;:–-")
    s = re.sub(r"\bm\.\s*", "міста ", s, flags=re.IGNORECASE)  # normalize "м."
    if len(s) > 10 and s.upper() == s:
        s = s.capitalize()
    return s

def _to_bool(v: Any) -> str:
    # For CSV: return "true"/"false"
    return "true" if (isinstance(v, bool) and v) else "false"

def build_json_schema(fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    required: List[str] = []
    for f in fields:
        t = f["type"]
        if t == "string":
            props[f["name"]] = {"type": "string", "description": f.get("description", "")}
        elif t == "number":
            props[f["name"]] = {"type": "number", "description": f.get("description", "")}
        elif t == "boolean":
            props[f["name"]] = {"type": "boolean", "description": f.get("description", "")}
        elif t == "string_array":
            props[f["name"]] = {"type": "array", "items": {"type": "string"}, "description": f.get("description", "")}
        else:
            raise ValueError(f"Unsupported field type: {t}")
        if f.get("required", False):
            required.append(f["name"])

    return {
        "name": "ua_awol_extraction_schema",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": props,
            "required": required,
            "additionalProperties": False
        }
    }

def build_prompts(fields: List[Dict[str, Any]], text: str) -> Tuple[str, str]:
    # System prompt: строго JSON
    sys_prompt = (
        "Ти витягуєш метадані з українських судових рішень. "
        "Поверни РІВНО JSON-об'єкт із запитаними полями. "
        "Жодного іншого тексту — тільки валідний JSON."
    )

    # Field instructions (укр.)
    field_lines = ["Витягни такі поля (дотримуйся описів і форматів):"]
    for f in fields:
        kind_ua = {
            "string": "рядок",
            "number": "число",
            "boolean": "булеве значення (true/false)",
            "string_array": "масив рядків"
        }[f["type"]]
        req = " (обов'язково)" if f.get("required", False) else ""
        field_lines.append(f"- {f['name']} [{kind_ua}]{req}: {f.get('description', '')}")

    awol_rules = (
        "Правила нормалізації для СЗЧ (AWOL):\n"
        "- awol_start_date_iso / return_date_iso: формат РІВНО YYYY-MM-DD (ігноруй час та слова типу 'року').\n"
        "- awol_start_time_hhmm / return_time_hhmm: формат РІВНО HH:MM (24-годинний). "
        "Якщо час вказано приблизно — округли до найближчої хвилини.\n"
        "- Якщо якесь поле неможливо визначити — порожній рядок (для рядків) або false/0 (для булевих/числових).\n"
        "- 'раніше не судимого' => previously_convicted = false; будь-які згадки про попередні вироки => true."
    )

    example = (
        "Приклад коректного JSON (ключі мають бути саме такими, значення можуть бути порожніми):\n"
        "{\n"
        '  "awol_start_date_iso": "2023-01-21",\n'
        '  "awol_start_time_hhmm": "08:30",\n'
        '  "return_date_iso": "2023-04-14",\n'
        '  "return_time_hhmm": "12:30",\n'
        "}\n"
    )

    user_prompt = (
        "Задача: знайди та поверни запитані поля. Поверни лише JSON.\n\n"
        + "\n".join(field_lines) + "\n\n"
        + awol_rules + "\n\n"
        + example + "\n"
        + "Текст документу (може бути обрізаний):\n\n" + text
    )

    return sys_prompt, user_prompt

def llm_extract_fields(text: str) -> Dict[str, Any]:
    schema = build_json_schema(FIELDS)
    sys_prompt, user_prompt = build_prompts(FIELDS, text)

    # Prefer Responses API + JSON Schema
    try:
        resp = client.responses.create(
            model=MODEL,
            input=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_schema", "json_schema": schema},
            timeout=TIMEOUT_S,
        )
        try:
            data = resp.output[0].content[0].json
        except Exception:
            import json
            data = json.loads(resp.output_text)
    except TypeError:
        # Old SDK fallback: Chat Completions with JSON object (messages explicitly mention JSON)
        chat = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            timeout=TIMEOUT_S,
        )
        import json
        data = json.loads(chat.choices[0].message.content)

    # Ensure all keys exist + light cleanup
    for f in FIELDS:
        k = f["name"]
        t = f["type"]
        if k not in data:
            if t == "boolean":
                data[k] = False
            elif t == "number":
                data[k] = 0
            elif t == "string_array":
                data[k] = []
            else:
                data[k] = ""
        if t == "string":
            data[k] = _cleanup_text(data[k])
        elif t == "string_array" and isinstance(data[k], list):
            data[k] = [_cleanup_text(x) for x in data[k] if isinstance(x, str)]
        elif t == "boolean":
            data[k] = bool(data[k])
        elif t == "number":
            # try to coerce to int if it's a string like "2" or "2.0"
            try:
                v = data[k]
                if isinstance(v, str) and v.strip():
                    if re.fullmatch(r"-?\d+(\.0+)?", v.strip()):
                        data[k] = int(float(v.strip()))
                    else:
                        data[k] = float(v.strip())
                # if float is integral, cast to int
                if isinstance(data[k], float) and data[k].is_integer():
                    data[k] = int(data[k])
            except Exception:
                data[k] = 0
    return data

def process_file(path: Path) -> Dict[str, Any]:
    text = load_text_excerpt(path)
    fields = llm_extract_fields(text)
    row: Dict[str, Any] = {"file_path": str(path)}
    for f in FIELDS:
        k = f["name"]
        v = fields.get(k)
        if f["type"] == "string_array":
            row[k] = "; ".join(v) if isinstance(v, list) else ""
        elif f["type"] == "boolean":
            row[k] = _to_bool(v)
        else:
            row[k] = "" if v is None else str(v)
    return row

# --------- RESUME HELPERS (robust to BOM/case/separators) ---------
def _normalize_path_for_key(p: Path) -> str:
    """
    Make a stable key for comparing file paths across runs.
    Use absolute, resolved path; lower-case on Windows; normalize slashes.
    """
    try:
        rp = p.resolve()
    except Exception:
        rp = p
    key = str(rp)
    if os.name == "nt":
        key = key.lower().replace("/", "\\")
    else:
        key = key.replace("\\", "/")
    return key

def load_already_done_paths(csv_path: Path) -> set:
    """
    Load processed file paths from an existing OUTPUT_CSV (if present).
    Robust to BOM in header and to extra whitespace/quotes in values.
    """
    done = set()
    if not csv_path.exists():
        return done

    try:
        # utf-8-sig handles BOM, which is common on Windows CSVs
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []

            # Map potentially BOM-ed header back to plain 'file_path'
            header_map = {name.lstrip("\ufeff"): name for name in fieldnames}
            fp_key = header_map.get("file_path", "file_path")

            if fp_key not in fieldnames:
                # If the CSV lacks a usable header, treat as empty
                return done

            for row in reader:
                fp = (row.get(fp_key) or "").strip().strip('"').strip("'")
                if fp:
                    key = _normalize_path_for_key(Path(fp))
                    done.add(key)
    except Exception as e:
        print(f"Warning: could not read existing CSV '{csv_path}': {e}")
    return done

def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def main():
    input_dir = Path(INPUT_DIR)
    out_csv = Path(OUTPUT_CSV)

    # Gather all candidate HTML files
    all_files = sorted(
        [p for p in input_dir.rglob("*.html")] +
        [p for p in input_dir.rglob("*.htm")]
    )

    if not all_files:
        print(f"No HTML files found under: {INPUT_DIR}")
        return

    # Load already processed paths and filter
    already_done = load_already_done_paths(out_csv)
    candidates: List[Path] = []
    for p in all_files:
        key = _normalize_path_for_key(p)
        if key not in already_done:
            candidates.append(p)

    if not candidates:
        print("Nothing to do. All files in INPUT_DIR already exist in OUTPUT_CSV.")
        return

    total_remaining = len(candidates)
    to_process = candidates[:MAX_FILES] if MAX_FILES else candidates

    print(f"Found {len(all_files)} files; {len(already_done)} already in CSV; {total_remaining} remaining.")
    print(f"Processing up to {len(to_process)} files this run.")

    # Prepare CSV writer in append mode (header only if file does not exist)
    ensure_parent_dir(out_csv)
    write_header = not out_csv.exists()

    fieldnames = ["file_path"] + [f["name"] for f in FIELDS]
    processed_count = 0

    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for i, path in enumerate(to_process, 1):
            try:
                row = process_file(path)
                writer.writerow(row)
                processed_count += 1
                # brief status: show person label if present, else AWOL start
                label = row.get("person_label") or "—"
                when = row.get("awol_start_date_iso") or ""
                print(f"[{i}/{len(to_process)} of {total_remaining} remaining] {path.name}: {label} {when}")
            except Exception as e:
                print(f"[{i}/{len(to_process)} of {total_remaining} remaining] {path.name}: ERROR {e}")

    print(f"\nDone. Appended {processed_count} rows to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
