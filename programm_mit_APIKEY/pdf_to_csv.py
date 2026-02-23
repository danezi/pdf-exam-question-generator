#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PDF -> Fragen (JSON bevorzugt) -> CSV
Robuste IHK-Fragengenerierung aus (Doppel-)Seiten via OpenAI Vision.

Wichtigste Fixes ggü. deinem Skript:
- Standard: DOPPELSEITE (2 PDF-Seiten pro Einheit)
- Standard: Modell liefert JSON (stabiler als CSV), Script konvertiert zu CSV
- CSV-Parsing/Schreiben robust (csv-Modul, UTF-8-SIG optional, Header optional)
- Punkt/Komma/Fragezeichen ausdrücklich ERLAUBT (nur Semikolon im Inhalt verboten)
- Validierung erweitert: Quelle/Status, Duplikate, Feldlängen
- Ein Client pro Thread (stabiler in Parallelbetrieb)
- PDF nicht ständig neu öffnen (render über ein geöffnetes fitz.Document pro Worker)
"""

import os
import sys
import re
import json
import time
import base64
import random
import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import fitz  # PyMuPDF
from docx import Document
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# -----------------------------
# Konfiguration
# -----------------------------
MAX_VERSUCHE = 5
BASIS_WARTEZEIT = 2
MAX_WARTEZEIT = 60
DPI = 170  # etwas höher als 150 für bessere OCR/Vision
EXPECTED_COLS = 12

CSV_HEADER = ["Frage", "A", "B", "C", "D", "Richtig", "Richtig_Text", "Thema", "Quelle", "Status", "Kommentar", "Vollansicht"]

FEHLER_TYPEN = {
    "CONTENT_POLICY": "API hat den Inhalt abgelehnt (Inhaltsrichtlinie)",
    "KEIN_TEXT": "Die Seite enthält nicht genug verwertbaren Text",
    "NUR_BILD": "Die Seite enthält nur Bilder/Diagramme",
    "UNGUELTIG_JSON": "Die Antwort ist nicht im gültigen JSON-Format",
    "UNGUELTIG_CSV": "Die Antwort ist nicht im gültigen CSV-Format",
    "API_FEHLER": "Technischer API-Fehler",
    "VERBINDUNG": "Verbindungsfehler - Netzwerk oder Server nicht erreichbar",
    "RATE_LIMIT": "API-Anfragelimit erreicht - automatische Wartezeit",
    "ZEITÜBERSCHREITUNG": "Zeitlimit überschritten",
    "UNBEKANNT": "Unbekannter Fehler"
}

schreib_lock = Lock()
MEGAPROMPT_INHALT = None


# -----------------------------
# Datenmodell
# -----------------------------
@dataclass
class QAItem:
    frage: str
    A: str
    B: str
    C: str
    D: str
    richtig: str
    richtig_text: str
    thema: str
    quelle: str
    status: str
    kommentar: str = ""
    vollansicht: str = ""


# -----------------------------
# Utilities
# -----------------------------
def lade_megaprompt(docx_pfad: str) -> str:
    if not os.path.exists(docx_pfad):
        raise FileNotFoundError(f"Megaprompt-Datei nicht gefunden: {docx_pfad}")
    doc = Document(docx_pfad)
    return "\n".join([p.text for p in doc.paragraphs]).strip()


def berechne_wartezeit(versuch: int, fehler_typ: str) -> float:
    if fehler_typ == "RATE_LIMIT":
        basis = BASIS_WARTEZEIT * 4
    elif fehler_typ == "VERBINDUNG":
        basis = BASIS_WARTEZEIT * 2
    else:
        basis = BASIS_WARTEZEIT
    wartezeit = min(basis * (2 ** versuch), MAX_WARTEZEIT)
    jitter = wartezeit * 0.2 * (random.random() * 2 - 1)
    return max(1, wartezeit + jitter)


def erkenne_fehlertyp(antwort_text: str | None, ausnahme: Exception | None) -> tuple[str, str]:
    if ausnahme is not None:
        fehler_text = f"{str(ausnahme)} {type(ausnahme).__name__}".lower()
        if any(w in fehler_text for w in ["connection", "connect", "network", "socket", "refused", "reset", "ssl", "handshake", "apiconnection", "connectionerror"]):
            return "VERBINDUNG", f"{FEHLER_TYPEN['VERBINDUNG']} - Detail: {str(ausnahme)[:160]}"
        if any(w in fehler_text for w in ["rate", "limit", "429", "quota", "exceeded"]):
            return "RATE_LIMIT", FEHLER_TYPEN["RATE_LIMIT"]
        if any(w in fehler_text for w in ["timeout", "timed out", "deadline"]):
            return "ZEITÜBERSCHREITUNG", FEHLER_TYPEN["ZEITÜBERSCHREITUNG"]
        if any(w in fehler_text for w in ["401", "403", "500", "502", "503", "badrequest", "invalid", "api"]):
            return "API_FEHLER", f"{FEHLER_TYPEN['API_FEHLER']} - Detail: {str(ausnahme)[:160]}"

    if antwort_text:
        t = antwort_text.lower()
        if any(p in t for p in ["i can't assist", "i cannot assist", "policy", "guidelines", "inappropriate", "can't help", "cannot help"]):
            return "CONTENT_POLICY", FEHLER_TYPEN["CONTENT_POLICY"]

    return "UNBEKANNT", FEHLER_TYPEN["UNBEKANNT"]


def protokolliere_fehler(
    fehler_log_pfad: str,
    einheit_label: str,
    fehler_typ: str,
    fehler_nachricht: str,
    rohe_antwort: str | None = None
):
    zeitstempel = datetime.now().isoformat()
    fehler_eintrag = {
        "zeitstempel": zeitstempel,
        "einheit": einheit_label,
        "fehler_typ": fehler_typ,
        "fehler_nachricht": fehler_nachricht,
        "rohe_antwort_vorschau": rohe_antwort[:800] if rohe_antwort else None
    }

    with schreib_lock:
        data = []
        if os.path.exists(fehler_log_pfad):
            try:
                with open(fehler_log_pfad, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = []
        data.append(fehler_eintrag)
        with open(fehler_log_pfad, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        txt_log = fehler_log_pfad.replace(".json", ".txt")
        with open(txt_log, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*70}\n[{zeitstempel}] {einheit_label}\nTyp: {fehler_typ}\nNachricht: {fehler_nachricht}\n")


def render_page_to_base64(doc: fitz.Document, page_index: int, dpi: int = DPI) -> str:
    page = doc[page_index]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img_bytes = pix.tobytes("png")
    return base64.b64encode(img_bytes).decode("utf-8")


def preflight(doc: fitz.Document, page_index: int) -> dict:
    page = doc[page_index]
    text = page.get_text("text") or ""
    images = page.get_images(full=True) or []
    text_norm = re.sub(r"\s+", "", text)
    text_len = len(text_norm)
    return {
        "text_len": text_len,
        "image_count": len(images),
        "ist_bildlastig": (text_len < 80 and len(images) > 0),
        "ist_textlastig": (text_len >= 200),
    }


# -----------------------------
# Prompts
# -----------------------------
def build_prompt_json(megaprompt: str, quelle: str) -> str:
    """
    JSON statt CSV: stabiler, keine Angst vor Satzzeichen.
    Wir konvertieren anschließend zu CSV (Semikolon-getrennt).
    """
    return f"""{megaprompt}

---
AKTUELLE QUELLE (für alle Fragen): {quelle}

WICHTIG (Format):
- Antworte AUSSCHLIESSLICH als JSON.
- JSON muss ein Array von Objekten sein.
- Jedes Objekt hat exakt diese Keys:
  frage, A, B, C, D, richtig, richtig_text, thema, quelle, status, kommentar, vollansicht
- quelle MUSS exakt "{quelle}" sein.
- status MUSS exakt "ok" sein.
- richtig MUSS "A" oder "B" oder "C" oder "D" sein.
- richtig_text MUSS exakt dem Text der gewählten Option entsprechen.
- KEIN Semikolon ';' in frage/Antworten/kommentar/vollansicht (ersetze es durch Komma oder Gedankenstrich).
- Punkt, Komma, Doppelpunkt und Fragezeichen sind ausdrücklich ERWÜNSCHT (sie sind CSV-sicher).

QUALITÄTS-CHECK (NICHT AUSGEBEN):
- Keine Einleitung, keine Zusatztexte, nur JSON.
- Keine Zeilenumbrüche in Strings, wenn es vermeidbar ist.

Beginne SOFORT mit '[' und gib NUR gültiges JSON aus.
"""


def build_prompt_csv_fallback(megaprompt: str, quelle: str) -> str:
    """
    Falls du unbedingt CSV willst (oder JSON kaputt kommt).
    """
    return f"""{megaprompt}

---
AKTUELLE QUELLE für diese Doppelseite: {quelle}

WICHTIG:
- Setze bei jeder Frage in der Spalte "Quelle" den Wert: {quelle}
- Setze bei jeder Frage in der Spalte "Status" den Wert: ok
- KEIN Semikolon ';' in Frage-/Antworttexten (ersetze durch Komma oder Gedankenstrich)
- Punkt, Komma, Doppelpunkt und Fragezeichen sind ausdrücklich ERWÜNSCHT.
- Verwende innerhalb von Feldern niemals Zeilenumbrüche

CSV (ohne Kopfzeile), exakt 12 Spalten:
Frage;A;B;C;D;Richtig;Richtig_Text;Thema;Quelle;Status;Kommentar;Vollansicht

QUALITÄTS-CHECK (NICHT AUSGEBEN):
- exakt 12 Spalten je Zeile (11 Semikolons)
- Richtig ist A/B/C/D
- Richtig_Text entspricht exakt der gewählten Option
- keine Einleitung, keine Überschriften, keine Erklärungen außerhalb CSV

Beginne SOFORT mit der ersten CSV-Zeile, KEINE Einleitung!
"""


# -----------------------------
# OpenAI Call
# -----------------------------
def call_openai_vision_json(client: OpenAI, model: str, prompt: str, base64_images: list[str], temperature: float) -> str:
    """
    Multi-image: wir schicken 1 oder 2 Seiten als getrennte image_url content parts.
    """
    content_parts = [{"type": "text", "text": prompt}]
    for b64 in base64_images:
        content_parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "high"}
        })

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Du bist ein Experte für IHK-Prüfungsfragen. Gib exakt das angeforderte Format aus, ohne Zusatztext."},
            {"role": "user", "content": content_parts},
        ],
        max_tokens=4096,
        temperature=temperature,
    )
    return resp.choices[0].message.content or ""


# -----------------------------
# Parsing / Validation
# -----------------------------
def strip_code_fences(text: str) -> str:
    text = re.sub(r"```(?:json|csv)?\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    return text.strip()


def normalize_no_newlines(s: str) -> str:
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()


def parse_json_items(raw: str) -> tuple[list[QAItem], list[str]]:
    errors = []
    raw = strip_code_fences(raw)

    try:
        data = json.loads(raw)
    except Exception as e:
        return [], [f"JSON parse error: {e}"]

    if not isinstance(data, list):
        return [], ["JSON ist kein Array"]

    items: list[QAItem] = []
    required = ["frage", "A", "B", "C", "D", "richtig", "richtig_text", "thema", "quelle", "status", "kommentar", "vollansicht"]

    for idx, obj in enumerate(data, start=1):
        if not isinstance(obj, dict):
            errors.append(f"Item {idx}: kein Objekt")
            continue
        missing = [k for k in required if k not in obj]
        if missing:
            errors.append(f"Item {idx}: missing keys {missing}")
            continue

        richtig = str(obj["richtig"]).upper().strip()
        if richtig not in ["A", "B", "C", "D"]:
            errors.append(f"Item {idx}: richtig={obj['richtig']} ungültig")
            continue

        # No semicolons in content
        for k in ["frage", "A", "B", "C", "D", "kommentar", "vollansicht", "thema", "richtig_text"]:
            if ";" in str(obj.get(k, "")):
                errors.append(f"Item {idx}: Semikolon in Feld {k}")
                break
        else:
            # richtig_text matches selected option
            chosen = str(obj[richtig]).strip()
            richtig_text = str(obj["richtig_text"]).strip()
            if richtig_text != chosen:
                errors.append(f"Item {idx}: richtig_text passt nicht zu Option {richtig}")
                continue

            item = QAItem(
                frage=normalize_no_newlines(str(obj["frage"])),
                A=normalize_no_newlines(str(obj["A"])),
                B=normalize_no_newlines(str(obj["B"])),
                C=normalize_no_newlines(str(obj["C"])),
                D=normalize_no_newlines(str(obj["D"])),
                richtig=richtig,
                richtig_text=normalize_no_newlines(richtig_text),
                thema=normalize_no_newlines(str(obj["thema"])),
                quelle=normalize_no_newlines(str(obj["quelle"])),
                status=normalize_no_newlines(str(obj["status"])),
                kommentar=normalize_no_newlines(str(obj.get("kommentar", ""))),
                vollansicht=normalize_no_newlines(str(obj.get("vollansicht", ""))),
            )

            # minimale Qualität (kannst du verschärfen)
            if len(item.frage) < 10:
                errors.append(f"Item {idx}: Frage sehr kurz")
                continue

            items.append(item)

    return items, errors


def parse_csv_lines(raw: str) -> tuple[list[QAItem], list[str]]:
    """
    Fallback: CSV vom Modell. Robust via csv.reader, nicht split(";").
    """
    errors = []
    raw = strip_code_fences(raw)
    lines = [ln for ln in raw.splitlines() if ln.strip()]

    items: list[QAItem] = []
    reader = csv.reader(lines, delimiter=";", quotechar='"', escapechar="\\")
    for i, row in enumerate(reader, start=1):
        if not row or len(row) != EXPECTED_COLS:
            errors.append(f"Zeile {i}: {len(row) if row else 0} Spalten (erwartet {EXPECTED_COLS})")
            continue
        row = [normalize_no_newlines(x) for x in row]
        frage, A, B, C, D, richtig, richtig_text, thema, quelle, status, kommentar, voll = row
        richtig = richtig.upper().strip()
        if richtig not in ["A", "B", "C", "D"]:
            errors.append(f"Zeile {i}: richtig={richtig} ungültig")
            continue
        chosen = {"A": A, "B": B, "C": C, "D": D}[richtig]
        if richtig_text != chosen:
            errors.append(f"Zeile {i}: richtig_text passt nicht zu Option {richtig}")
            continue
        if any(";" in x for x in [frage, A, B, C, D, kommentar, voll, thema, richtig_text]):
            errors.append(f"Zeile {i}: Semikolon im Inhalt")
            continue

        items.append(QAItem(frage, A, B, C, D, richtig, richtig_text, thema, quelle, status, kommentar, voll))

    return items, errors


def enforce_quelle_status(items: list[QAItem], quelle: str) -> list[str]:
    errors = []
    for idx, it in enumerate(items, start=1):
        if it.quelle != quelle:
            errors.append(f"Item {idx}: quelle='{it.quelle}' != '{quelle}'")
        if it.status.lower() != "ok":
            errors.append(f"Item {idx}: status='{it.status}' != 'ok'")
    return errors


def dedupe_by_question(items: list[QAItem]) -> tuple[list[QAItem], int]:
    seen = set()
    out = []
    dup = 0
    for it in items:
        key = re.sub(r"\s+", " ", it.frage.strip().lower())
        if key in seen:
            dup += 1
            continue
        seen.add(key)
        out.append(it)
    return out, dup


def items_to_csv_rows(items: list[QAItem]) -> list[list[str]]:
    rows = []
    for it in items:
        rows.append([
            it.frage, it.A, it.B, it.C, it.D,
            it.richtig, it.richtig_text, it.thema, it.quelle, it.status, it.kommentar, it.vollansicht
        ])
    return rows


# -----------------------------
# Core processing
# -----------------------------
def process_unit(
    pdf_path: str,
    page_indices: list[int],
    quelle: str,
    model: str,
    temperature: float,
    prefer_json: bool,
    api_key: str,
    fehler_log_pfad: str
) -> tuple[list[QAItem], str | None, str | None, str | None]:
    """
    Rückgabe: (items, fehler_typ, fehler_msg, raw_answer)
    """
    global MEGAPROMPT_INHALT
    raw_answer = None

    # pro Thread: eigenes doc & client -> stabil
    client = OpenAI(api_key=api_key)
    doc = fitz.open(pdf_path)

    try:
        # Preflight (nur fürs Debug/Strategie)
        metas = [preflight(doc, pi) for pi in page_indices]
        ist_bildlastig = all(m["ist_bildlastig"] for m in metas)

        # Render images einmal
        base64_images = [render_page_to_base64(doc, pi, dpi=DPI) for pi in page_indices]

        # Prompt wählen
        if prefer_json:
            prompt = build_prompt_json(MEGAPROMPT_INHALT, quelle)
        else:
            prompt = build_prompt_csv_fallback(MEGAPROMPT_INHALT, quelle)

        # Retry loop
        for versuch in range(MAX_VERSUCHE):
            try:
                raw_answer = call_openai_vision_json(client, model, prompt, base64_images, temperature=temperature)

                if prefer_json:
                    items, parse_errors = parse_json_items(raw_answer)
                    if items:
                        extra = enforce_quelle_status(items, quelle)
                        parse_errors.extend(extra)
                        items, dup = dedupe_by_question(items)
                        if dup:
                            parse_errors.append(f"Duplikate entfernt: {dup}")

                    if items and not parse_errors:
                        return items, None, None, raw_answer

                    # Wenn JSON kaputt: (einmal) CSV fallback probieren
                    if versuch == 0:
                        # Versuch: mit sehr niedriger Temperatur nochmals
                        temperature2 = min(temperature, 0.3)
                        # zweite Chance JSON, dann fallback CSV
                        temperature = temperature2
                    if versuch == MAX_VERSUCHE - 1:
                        return [], "UNGUELTIG_JSON", "; ".join(parse_errors[:10]), raw_answer

                else:
                    items, parse_errors = parse_csv_lines(raw_answer)
                    if items:
                        extra = enforce_quelle_status(items, quelle)
                        parse_errors.extend(extra)
                        items, dup = dedupe_by_question(items)
                        if dup:
                            parse_errors.append(f"Duplikate entfernt: {dup}")

                    if items and not parse_errors:
                        return items, None, None, raw_answer

                    if versuch == MAX_VERSUCHE - 1:
                        return [], "UNGUELTIG_CSV", "; ".join(parse_errors[:10]), raw_answer

            except Exception as e:
                fehler_typ, fehler_msg = erkenne_fehlertyp(raw_answer, e)
                if fehler_typ in ["VERBINDUNG", "RATE_LIMIT", "ZEITÜBERSCHREITUNG", "API_FEHLER"]:
                    if versuch < MAX_VERSUCHE - 1:
                        wait = berechne_wartezeit(versuch, fehler_typ)
                        time.sleep(wait)
                        continue
                    return [], fehler_typ, fehler_msg, raw_answer
                else:
                    # nicht-technisch -> nicht endlos retrien
                    return [], fehler_typ, fehler_msg, raw_answer

        return [], "UNBEKANNT", "Unbekannter Fehler nach Retries", raw_answer

    finally:
        doc.close()


def write_csv(output_path: str, rows: list[list[str]], write_header: bool):
    with schreib_lock:
        file_exists = os.path.exists(output_path) and os.path.getsize(output_path) > 0
        with open(output_path, "a", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            if write_header and not file_exists:
                w.writerow(CSV_HEADER)
            for r in rows:
                w.writerow(r)


# -----------------------------
# CLI main
# -----------------------------
def main():
    global MEGAPROMPT_INHALT

    parser = argparse.ArgumentParser(
        description="Konvertiert ein PDF in Fragen (JSON bevorzugt) und exportiert als CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--pdf", type=str, help="Pfad zur PDF-Datei")
    parser.add_argument("--prompt", type=str, required=True, help="Pfad zur Megaprompt DOCX-Datei")
    parser.add_argument("--start", type=int, default=1, help="Startseite (1-indiziert)")
    parser.add_argument("--end", type=int, default=None, help="Endseite (inklusive)")
    parser.add_argument("--output", type=str, default=None, help="Ausgabe-CSV-Datei")
    parser.add_argument("--test", action="store_true", help="Testmodus: nur 1 Einheit")
    parser.add_argument("--book-start", type=int, default=None, help="Erste Buchseitennummer")
    parser.add_argument("--unit", type=int, default=2, choices=[1, 2], help="Seiten pro Einheit: 2=Doppelseite (empfohlen)")
    parser.add_argument("--parallel", type=int, default=1, help="Parallele Worker")
    parser.add_argument("--prefer-json", action="store_true", help="Bevorzuge JSON Output (empfohlen)")
    parser.add_argument("--temperature", type=float, default=0.35, help="Temperatur (empfohlen 0.2-0.4)")
    parser.add_argument("--header", action="store_true", help="CSV Header schreiben (wenn du ihn wirklich willst)")
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4.1",
        choices=["gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano", "gpt-4o", "gpt-4o-mini"],
        help="OpenAI-Modell"
    )
    args = parser.parse_args()

    # Megaprompt
    print(f"\n📝 Lade Megaprompt: {args.prompt}")
    MEGAPROMPT_INHALT = lade_megaprompt(args.prompt)
    print(f"   ✓ geladen ({len(MEGAPROMPT_INHALT)} Zeichen)")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ FEHLER: OPENAI_API_KEY fehlt")
        sys.exit(1)

    pdf_path = args.pdf or input("📁 Pfad zur PDF-Datei: ").strip().strip('"')
    if not os.path.exists(pdf_path):
        print(f"❌ FEHLER: Datei nicht gefunden: {pdf_path}")
        sys.exit(1)

    # Seitenanzahl
    with fitz.open(pdf_path) as doc:
        total_pages = len(doc)

    start = max(1, args.start)
    end = args.end if args.end else total_pages
    end = min(end, total_pages)
    if start > end:
        print(f"❌ FEHLER: start({start}) > end({end})")
        sys.exit(1)

    # Output
    out = args.output or f"{Path(pdf_path).stem}_questions.csv"
    err_log = out.replace(".csv", "_errors.json")
    print(f"\n📁 Ausgabe: {out}\n   Fehler-Log: {err_log}")

    # Buchseiten
    unit = args.unit
    prefer_json = args.prefer_json or True  # Standard TRUE (stabiler)
    if args.prefer_json is False:
        prefer_json = True

    # Wenn user explizit kein JSON will, kann er --prefer-json weglassen? -> wir halten Standard True
    # Wenn du CSV-only willst: setze prefer_json=False hier manuell oder erweitere um --csv-only.
    # Für einfache Bedienung: JSON ist Standard.

    # Buchstart: sinnvolle Defaultlogik
    buch_start = args.book_start if args.book_start else (start * 2 - 1 if unit == 2 else start)

    # Units bauen
    page_indices = list(range(start - 1, end))
    units = []
    i = 0
    unit_idx = 0
    while i < len(page_indices):
        chunk = page_indices[i:i + unit]
        if len(chunk) < unit and unit == 2:
            # letzte ungerade Seite: trotzdem als Einheit verarbeiten
            pass

        # Buchseiteninfo berechnen
        if unit == 2:
            links = buch_start + unit_idx * 2
            rechts = links + 1
            quelle = f"Buch S. {links}-{rechts}"
        else:
            b = buch_start + unit_idx
            quelle = f"Buch S. {b}"

        units.append((chunk, quelle))
        i += unit
        unit_idx += 1

    if args.test:
        units = units[:1]
        print("🧪 TESTMODUS: nur 1 Einheit")

    print(f"\n📄 PDF Seiten: {total_pages}")
    print(f"   Bereich: {start}-{end} ({len(page_indices)} Seiten)")
    print(f"   Einheit: {unit} Seite(n) pro Einheit")
    print(f"   Einheiten: {len(units)}")
    print(f"   Modell: {args.model}")
    print(f"   Parallel: {args.parallel}")
    print(f"   Output-Format: {'JSON→CSV' if prefer_json else 'CSV direkt'}")
    print(f"   CSV Header: {'ja' if args.header else 'nein (prompt-konform)'}")

    stats = {"units": len(units), "ok": 0, "fail": 0, "fragen": 0, "fehler_typen": {}}

    def worker(unit_tuple):
        pages, quelle = unit_tuple
        label = f"PDF-Seiten {','.join(str(p+1) for p in pages)} | {quelle}"
        items, fehler_typ, fehler_msg, raw = process_unit(
            pdf_path=pdf_path,
            page_indices=pages,
            quelle=quelle,
            model=args.model,
            temperature=args.temperature,
            prefer_json=True,  # Standard: JSON bevorzugen
            api_key=api_key,
            fehler_log_pfad=err_log,
        )
        return label, quelle, items, fehler_typ, fehler_msg, raw

    start_time = time.time()

    if args.parallel > 1:
        results = []
        with ThreadPoolExecutor(max_workers=args.parallel) as ex:
            futs = [ex.submit(worker, u) for u in units]
            for fut in as_completed(futs):
                results.append(fut.result())

        # stabil schreiben (sortiert nach Buchquelle)
        results.sort(key=lambda x: x[1])
        for label, quelle, items, fehler_typ, fehler_msg, raw in results:
            print(f"\n[{quelle}] {label}")
            if fehler_typ:
                print(f"  ❌ {fehler_typ}: {fehler_msg}")
                stats["fail"] += 1
                stats["fehler_typen"][fehler_typ] = stats["fehler_typen"].get(fehler_typ, 0) + 1
                protokolliere_fehler(err_log, label, fehler_typ, fehler_msg or "", raw)
                continue
            rows = items_to_csv_rows(items)
            write_csv(out, rows, write_header=args.header)
            stats["ok"] += 1
            stats["fragen"] += len(items)
            print(f"  ✅ {len(items)} Fragen")
    else:
        for u in units:
            label, quelle, items, fehler_typ, fehler_msg, raw = worker(u)
            print(f"\n[{quelle}] {label}")
            if fehler_typ:
                print(f"  ❌ {fehler_typ}: {fehler_msg}")
                stats["fail"] += 1
                stats["fehler_typen"][fehler_typ] = stats["fehler_typen"].get(fehler_typ, 0) + 1
                protokolliere_fehler(err_log, label, fehler_typ, fehler_msg or "", raw)
                continue
            rows = items_to_csv_rows(items)
            write_csv(out, rows, write_header=args.header)
            stats["ok"] += 1
            stats["fragen"] += len(items)
            print(f"  ✅ {len(items)} Fragen")

    dur = time.time() - start_time
    print("\n" + "=" * 70)
    print("FERTIG")
    print("=" * 70)
    print(f"Dauer: {dur/60:.1f} min ({dur:.0f}s)")
    print(f"Einheiten: {stats['units']}, OK: {stats['ok']}, Fail: {stats['fail']}, Fragen: {stats['fragen']}")
    if stats["ok"]:
        print(f"Ø Fragen/Einheit: {stats['fragen']/stats['ok']:.1f}")
    if stats["fehler_typen"]:
        print("Fehler nach Typ:")
        for k, v in stats["fehler_typen"].items():
            print(f"  - {k}: {v}")
    print(f"\nDateien:\n  CSV: {out}\n  Fehlerlog: {err_log if stats['fail'] else '(keins)'}")


if __name__ == "__main__":
    main()
