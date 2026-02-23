#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PDF zu CSV - Automatisierung der IHK-Fragengenerierung
Konvertiert jede Seite eines PDFs in Single-Choice-Fragen via OpenAI GPT-5.2

Verwendung:
    python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx
    python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --start 5 --end 10
    python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --test
    python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --parallel 3
"""

import os
import sys
import argparse
import base64
import re
import time
import json
import random
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import fitz  # PyMuPDF
from docx import Document
from openai import OpenAI
from dotenv import load_dotenv

# Umgebungsvariablen laden
load_dotenv()

# Konfiguration
MAX_VERSUCHE = 5  # Erhöht für bessere Fehlertoleranz
BASIS_WARTEZEIT = 2  # Basis-Sekunden für exponentiellen Backoff
MAX_WARTEZEIT = 60  # Maximale Wartezeit
DPI = 150  # Auflösung für PDF-zu-Bild-Konvertierung

# CSV-Spaltenüberschriften (gemäß Megaprompt)
CSV_HEADER = "Frage;A;B;C;D;Richtig;Richtig_Text;Thema;Quelle;Status;Kommentar;Vollansicht"

# Erkannte Fehlertypen
FEHLER_TYPEN = {
    "CONTENT_POLICY": "API hat den Inhalt abgelehnt (Inhaltsrichtlinie)",
    "KEIN_TEXT": "Die Seite enthält nicht genug verwertbaren Text",
    "NUR_BILD": "Die Seite enthält nur Bilder/Diagramme",
    "UNGUELTIG_CSV": "Die Antwort ist nicht im gültigen CSV-Format",
    "API_FEHLER": "Technischer API-Fehler",
    "VERBINDUNG": "Verbindungsfehler - Netzwerk oder Server nicht erreichbar",
    "RATE_LIMIT": "API-Anfragelimit erreicht - automatische Wartezeit",
    "ZEITÜBERSCHREITUNG": "Zeitlimit überschritten",
    "UNBEKANNT": "Unbekannter Fehler"
}

# Thread-sicheres Schreiben
schreib_lock = Lock()

# Globale Variable für den geladenen Megaprompt
MEGAPROMPT_INHALT = None


def lade_megaprompt(docx_pfad: str) -> str:
    """Lädt den Megaprompt aus einer DOCX-Datei."""
    if not os.path.exists(docx_pfad):
        raise FileNotFoundError(f"Megaprompt-Datei nicht gefunden: {docx_pfad}")

    doc = Document(docx_pfad)
    text = '\n'.join([p.text for p in doc.paragraphs])
    return text


def get_megaprompt_mit_quelle(seiten_info: str) -> str:
    """Gibt den geladenen Megaprompt mit der Quellenangabe zurück."""
    global MEGAPROMPT_INHALT
    if not MEGAPROMPT_INHALT:
        raise ValueError("Megaprompt wurde nicht geladen!")

    # Füge die Quellenangabe und spezifische Anweisungen hinzu
    return f"""{MEGAPROMPT_INHALT}

---
AKTUELLE QUELLE für diese Doppelseite: {seiten_info}

WICHTIG:
- Setze bei jeder Frage in der Spalte "Quelle" den Wert: {seiten_info}
- Setze bei jeder Frage in der Spalte "Status" den Wert: ok
- KEINE Semikolons (;) im Textinhalt der Fragen/Antworten!
- Beginne SOFORT mit der ersten CSV-Zeile, KEINE Einleitung!"""


def get_standard_prompt(seiten_info: str) -> str:
    """Fallback-Prompt falls Megaprompt nicht funktioniert."""
    return f"""Erstelle 12-15 Single-Choice-Prüfungsfragen für IHK-Einzelhandel.

Regeln:
- 4 Antworten pro Frage (A-D), eine richtig
- Richtige Antwort zufällig verteilen
- KEINE Semikolons im Text
- Praxisnah und verständlich
- Gendergerechte Du-Form

Fallbeispiele mit: Frau Rabatta (Modehaus), Herr Andreh (Frischemarkt), Azubi Lisa, Azubi Mehmet

Format: CSV mit Semikolon, OHNE Kopfzeile
Spalten: Frage;A;B;C;D;Richtig;Richtig_Text;Thema;Quelle;Status;Kommentar;Vollansicht
Quelle: {seiten_info}
Status: ok

Nur CSV ausgeben, keine Erklärungen:"""


def get_notfall_prompt(seiten_info: str) -> str:
    """Minimaler Notfall-Prompt."""
    return f"""Erstelle 8 einfache Verständnisfragen zum Bildinhalt.

CSV-Format: Frage;A;B;C;D;Richtig;Richtig_Text;Thema;{seiten_info};ok;;Kurzfassung

Keine Semikolons im Text. Beginne direkt:"""


def erkenne_fehlertyp(antwort: str, ausnahme: Exception = None) -> tuple[str, str]:
    """Erkennt den Fehlertyp und gibt (Code, explizite Nachricht) zurück."""

    # Zuerst Ausnahme prüfen (wichtiger)
    if ausnahme:
        # Prüfe sowohl die Exception-Nachricht als auch den Typ
        fehler_text = str(ausnahme).lower()
        exception_typ = type(ausnahme).__name__.lower()

        # Kombiniere alle relevanten Texte für die Suche
        alle_texte = f"{fehler_text} {exception_typ}"

        # Prüfe auch __cause__ und __context__ für verschachtelte Exceptions
        if ausnahme.__cause__:
            alle_texte += f" {str(ausnahme.__cause__).lower()} {type(ausnahme.__cause__).__name__.lower()}"
        if ausnahme.__context__:
            alle_texte += f" {str(ausnahme.__context__).lower()} {type(ausnahme.__context__).__name__.lower()}"

        # Verbindungsfehler - erweiterte Erkennung
        verbindungs_keywords = [
            "connection", "connect", "network", "socket", "refused",
            "reset", "broken", "pipe", "eof", "ssl", "handshake",
            "apiconnection", "connectionerror", "remotedisconnected",
            "newconnectionerror", "maxretryerror", "urlerror"
        ]
        if any(w in alle_texte for w in verbindungs_keywords):
            return "VERBINDUNG", f"{FEHLER_TYPEN['VERBINDUNG']} - Detail: {str(ausnahme)[:100]}"

        # Rate Limit
        if any(w in alle_texte for w in ["rate", "limit", "429", "quota", "exceeded"]):
            return "RATE_LIMIT", FEHLER_TYPEN["RATE_LIMIT"]

        # Timeout
        if any(w in alle_texte for w in ["timeout", "timed out", "deadline"]):
            return "ZEITÜBERSCHREITUNG", FEHLER_TYPEN["ZEITÜBERSCHREITUNG"]

        # Allgemeiner API-Fehler
        if any(w in alle_texte for w in ["api", "invalid", "401", "403", "500", "502", "503", "badrequest"]):
            return "API_FEHLER", FEHLER_TYPEN["API_FEHLER"]

    if antwort:
        antwort_klein = antwort.lower()

        # Ablehnung wegen Inhaltsrichtlinie
        if any(phrase in antwort_klein for phrase in [
            "i can't assist", "i cannot assist", "i'm sorry", "i am sorry",
            "can't help", "cannot help", "not able to", "unable to",
            "policy", "guidelines", "inappropriate"
        ]):
            return "CONTENT_POLICY", FEHLER_TYPEN["CONTENT_POLICY"]

        # Seite ohne verwertbaren Text
        if any(phrase in antwort_klein for phrase in [
            "no text", "cannot read", "image only", "blank page",
            "empty page", "cannot extract"
        ]):
            return "KEIN_TEXT", FEHLER_TYPEN["KEIN_TEXT"]

        # Seite mit nur Bildern
        if any(phrase in antwort_klein for phrase in [
            "diagram", "chart", "figure", "illustration", "schematic", "graph"
        ]):
            return "NUR_BILD", FEHLER_TYPEN["NUR_BILD"]

    # Ungültiges CSV
    if antwort and ';' not in antwort:
        return "UNGUELTIG_CSV", FEHLER_TYPEN["UNGUELTIG_CSV"]

    return "UNBEKANNT", FEHLER_TYPEN["UNBEKANNT"]


def berechne_wartezeit(versuch: int, fehler_typ: str) -> float:
    """Berechnet exponentielle Wartezeit mit Jitter."""
    if fehler_typ == "RATE_LIMIT":
        # Längere Wartezeit bei Rate Limit
        basis = BASIS_WARTEZEIT * 4
    elif fehler_typ == "VERBINDUNG":
        # Mittlere Wartezeit bei Verbindungsproblemen
        basis = BASIS_WARTEZEIT * 2
    else:
        basis = BASIS_WARTEZEIT

    # Exponentieller Backoff: basis * 2^versuch
    wartezeit = min(basis * (2 ** versuch), MAX_WARTEZEIT)

    # Jitter hinzufügen (±20%)
    jitter = wartezeit * 0.2 * (random.random() * 2 - 1)

    return max(1, wartezeit + jitter)


def pdf_seite_zu_base64(pdf_pfad: str, seiten_nr: int, dpi: int = DPI) -> str:
    """Konvertiert eine PDF-Seite in ein Base64-codiertes Bild."""
    doc = fitz.open(pdf_pfad)
    seite = doc[seiten_nr]

    # In Bild mit angegebener Auflösung konvertieren
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = seite.get_pixmap(matrix=mat)

    # In PNG und dann in Base64 konvertieren
    bild_bytes = pix.tobytes("png")
    base64_bild = base64.b64encode(bild_bytes).decode('utf-8')

    doc.close()
    return base64_bild


def hole_pdf_seitenanzahl(pdf_pfad: str) -> int:
    """Gibt die Anzahl der Seiten im PDF zurück."""
    doc = fitz.open(pdf_pfad)
    anzahl = len(doc)
    doc.close()
    return anzahl


def rufe_openai_vision(client: OpenAI, base64_bild: str, prompt: str, modell: str = "gpt-5.2") -> str:
    """Ruft die OpenAI Vision API mit dem Bild auf."""

    # GPT-5 Modelle verwenden max_completion_tokens statt max_tokens
    ist_gpt5 = modell.startswith("gpt-5")

    api_params = {
        "model": modell,
        "messages": [
            {
                "role": "system",
                "content": "Du bist ein Experte für IHK-Prüfungsfragen. Antworte IMMER und NUR im CSV-Format. Keine Einleitungen, keine Erklärungen."
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{base64_bild}",
                            "detail": "high"
                        }
                    }
                ]
            }
        ],
        "temperature": 0.7,
        "timeout": 120  # 2 Minuten Timeout
    }

    # Richtigen Token-Parameter je nach Modell verwenden
    if ist_gpt5:
        api_params["max_completion_tokens"] = 4096
    else:
        api_params["max_tokens"] = 4096

    antwort = client.chat.completions.create(**api_params)

    return antwort.choices[0].message.content


def bereinige_und_validiere_csv(antwort: str) -> tuple[str, int, list]:
    """Bereinigt die API-Antwort, validiert und gibt (csv, anzahl_fragen, fehler) zurück."""
    if not antwort:
        return "", 0, ["Leere Antwort"]

    # Markdown-Codeblöcke entfernen
    antwort = re.sub(r'```csv\s*', '', antwort)
    antwort = re.sub(r'```\s*', '', antwort)

    zeilen = antwort.strip().split('\n')
    gueltige_zeilen = []
    fehler = []

    for i, zeile in enumerate(zeilen, 1):
        zeile = zeile.strip()
        if not zeile or zeile.startswith('#'):
            continue

        if ';' not in zeile:
            continue

        teile = zeile.split(';')

        # Mindestens 10 Spalten
        if len(teile) < 10:
            fehler.append(f"Zeile {i}: Nur {len(teile)} Spalten (mind. 10 erwartet)")
            continue

        # Spalte 6 (Richtig) muss A, B, C oder D sein
        if len(teile) > 5:
            richtig = teile[5].strip().upper()
            if richtig not in ['A', 'B', 'C', 'D']:
                fehler.append(f"Zeile {i}: Ungültige Antwort '{teile[5]}' (A/B/C/D erwartet)")
                continue
            # Korrigiere falls nötig
            teile[5] = richtig

        # Frage darf nicht leer sein
        if not teile[0].strip():
            fehler.append(f"Zeile {i}: Leere Frage")
            continue

        # Alle 4 Antworten müssen vorhanden sein
        for j in range(1, 5):
            if j < len(teile) and not teile[j].strip():
                fehler.append(f"Zeile {i}: Antwort {chr(64+j)} ist leer")
                break
        else:
            gueltige_zeilen.append(';'.join(teile))

    return '\n'.join(gueltige_zeilen), len(gueltige_zeilen), fehler


def protokolliere_fehler(fehler_log_pfad: str, pdf_seite: int, buch_seiten_info: str,
                         fehler_typ: str, fehler_nachricht: str, rohe_antwort: str = None):
    """Protokolliert einen Fehler in der Log-Datei (thread-sicher)."""
    zeitstempel = datetime.now().isoformat()

    fehler_eintrag = {
        "zeitstempel": zeitstempel,
        "pdf_seite": pdf_seite,
        "buch_seiten": buch_seiten_info,
        "fehler_typ": fehler_typ,
        "fehler_nachricht": fehler_nachricht,
        "rohe_antwort_vorschau": rohe_antwort[:500] if rohe_antwort else None
    }

    with schreib_lock:
        # JSON-Log
        fehler = []
        if os.path.exists(fehler_log_pfad):
            try:
                with open(fehler_log_pfad, 'r', encoding='utf-8') as f:
                    fehler = json.load(f)
            except:
                fehler = []

        fehler.append(fehler_eintrag)

        with open(fehler_log_pfad, 'w', encoding='utf-8') as f:
            json.dump(fehler, f, ensure_ascii=False, indent=2)

        # Text-Log
        txt_log_pfad = fehler_log_pfad.replace('.json', '.txt')
        with open(txt_log_pfad, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{zeitstempel}] PDF-Seite {pdf_seite} ({buch_seiten_info})\n")
            f.write(f"Typ: {fehler_typ}\n")
            f.write(f"Nachricht: {fehler_nachricht}\n")


def verarbeite_seite(client: OpenAI, pdf_pfad: str, seiten_nr: int, seiten_info: str,
                     modell: str = "gpt-4o", fehler_log_pfad: str = None) -> tuple[str, str, str, int]:
    """Verarbeitet eine PDF-Seite mit intelligentem Retry.

    Rückgabe:
        tuple: (csv_inhalt, fehler_nachricht, fehler_typ, anzahl_fragen)
    """
    letzte_antwort = None

    # Strategien mit Prompts unterschiedlicher Qualität
    # Megaprompt zuerst, dann Fallbacks
    strategien = [
        ("megaprompt", get_megaprompt_mit_quelle),
        ("standard", get_standard_prompt),
        ("notfall", get_notfall_prompt),
    ]

    for strategie_idx, (strategie_name, prompt_func) in enumerate(strategien):

        # Pro Strategie mehrere Versuche bei Verbindungsfehlern
        for versuch in range(MAX_VERSUCHE):
            try:
                if versuch == 0:
                    print(f"  [{strategie_name.upper()}] Versuch {versuch + 1}...")
                else:
                    print(f"  [{strategie_name.upper()}] Wiederholung {versuch + 1}/{MAX_VERSUCHE}...")

                # Seite in Bild konvertieren (nur einmal pro Strategie)
                if versuch == 0:
                    base64_bild = pdf_seite_zu_base64(pdf_pfad, seiten_nr)

                # Prompt holen
                prompt = prompt_func(seiten_info)

                # API aufrufen
                antwort = rufe_openai_vision(client, base64_bild, prompt, modell)
                letzte_antwort = antwort

                # Prüfen ob Ablehnung
                fehler_typ, fehler_msg = erkenne_fehlertyp(antwort)
                if fehler_typ in ["CONTENT_POLICY", "KEIN_TEXT", "NUR_BILD"]:
                    print(f"  ⚠️  {fehler_msg}")
                    if strategie_idx < len(strategien) - 1:
                        print(f"  → Wechsle zu nächster Strategie...")
                        break  # Nächste Strategie versuchen
                    else:
                        return None, fehler_msg, fehler_typ, 0

                # CSV bereinigen und validieren
                csv_inhalt, anzahl_fragen, validierungs_fehler = bereinige_und_validiere_csv(antwort)

                if validierungs_fehler:
                    print(f"  ⚠️  {len(validierungs_fehler)} Validierungsprobleme")

                if anzahl_fragen == 0:
                    if strategie_idx < len(strategien) - 1:
                        print(f"  → Keine gültigen Fragen, wechsle Strategie...")
                        break
                    else:
                        return None, "Keine gültigen CSV-Zeilen generiert", "UNGUELTIG_CSV", 0

                # Erfolg!
                print(f"  ✓ {anzahl_fragen} gültige Fragen")
                return csv_inhalt, None, None, anzahl_fragen

            except Exception as e:
                fehler_typ, fehler_msg = erkenne_fehlertyp(letzte_antwort, e)
                letzter_fehler = str(e)
                letzter_fehler_typ = fehler_typ

                # Debug: Zeige Exception-Typ für bessere Diagnose
                print(f"  [DEBUG] Exception-Typ: {type(e).__name__}")
                if e.__cause__:
                    print(f"  [DEBUG] __cause__: {type(e.__cause__).__name__}: {str(e.__cause__)[:80]}")

                # Bei Verbindungs- oder Rate-Limit-Fehlern: Retry mit derselben Strategie
                if fehler_typ in ["VERBINDUNG", "RATE_LIMIT", "ZEITÜBERSCHREITUNG", "API_FEHLER"]:
                    if versuch < MAX_VERSUCHE - 1:
                        wartezeit = berechne_wartezeit(versuch, fehler_typ)
                        print(f"  ⚠️  {fehler_typ}: {str(e)[:80]}...")
                        print(f"  ⏳ Warte {wartezeit:.1f}s vor nächstem Versuch...")
                        time.sleep(wartezeit)
                        continue
                    else:
                        # Max Versuche erreicht für diese Strategie
                        print(f"  ❌ {fehler_typ}: Max. Versuche erreicht für {strategie_name}")
                        return None, f"{fehler_msg}", fehler_typ, 0

                # Bei anderen Fehlern: Nächste Strategie
                print(f"  ❌ {fehler_typ}: {str(e)[:80]}")
                if strategie_idx < len(strategien) - 1:
                    print(f"  → Wechsle zu nächster Strategie...")
                    time.sleep(2)
                    break

    # Alle Strategien fehlgeschlagen - gib den letzten bekannten Fehler zurück
    return None, f"Alle Strategien fehlgeschlagen - Letzter Fehler: {letzter_fehler[:100] if 'letzter_fehler' in dir() else 'unbekannt'}", letzter_fehler_typ if 'letzter_fehler_typ' in dir() else "UNBEKANNT", 0


def verarbeite_seite_wrapper(args: tuple) -> dict:
    """Wrapper für parallele Verarbeitung."""
    client, pdf_pfad, seiten_nr, seiten_info, modell, fehler_log_pfad, pdf_seite, gesamt_seiten = args

    ergebnis = {
        "pdf_seite": pdf_seite,
        "seiten_info": seiten_info,
        "csv_inhalt": None,
        "fehler": None,
        "fehler_typ": None,
        "anzahl_fragen": 0
    }

    try:
        csv_inhalt, fehler, fehler_typ, anzahl = verarbeite_seite(
            client, pdf_pfad, seiten_nr, seiten_info, modell, fehler_log_pfad
        )
        ergebnis["csv_inhalt"] = csv_inhalt
        ergebnis["fehler"] = fehler
        ergebnis["fehler_typ"] = fehler_typ
        ergebnis["anzahl_fragen"] = anzahl
    except Exception as e:
        ergebnis["fehler"] = str(e)
        ergebnis["fehler_typ"] = "UNBEKANNT"

    return ergebnis


def main():
    global MEGAPROMPT_INHALT

    parser = argparse.ArgumentParser(
        description="Konvertiert ein PDF in Fragen-CSV via OpenAI GPT-5.2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx
  python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --start 5 --end 10
  python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --test
  python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --model gpt-4o
  python pdf_to_csv.py --pdf buch.pdf --prompt megaprompt.docx --parallel 3
        """
    )

    parser.add_argument('--pdf', type=str, help='Pfad zur PDF-Datei')
    parser.add_argument('--prompt', type=str, help='Pfad zur Megaprompt DOCX-Datei')
    parser.add_argument('--start', type=int, default=1, help='Startseite (1-indiziert, Standard: 1)')
    parser.add_argument('--end', type=int, default=None, help='Endseite (inklusive, Standard: letzte Seite)')
    parser.add_argument('--output', type=str, default=None, help='Ausgabe-CSV-Datei')
    parser.add_argument('--test', action='store_true', help='Testmodus: nur 2 Seiten')
    parser.add_argument('--book-start', type=int, default=None, help='Erste Buchseitennummer')
    parser.add_argument('--model', type=str, default='gpt-5.2',
                        choices=['gpt-5.2', 'gpt-5.2-pro', 'gpt-5.1', 'gpt-5-mini', 'gpt-4.1', 'gpt-4.1-mini', 'gpt-4o', 'gpt-4o-mini'],
                        help='OpenAI-Modell (Standard: gpt-5.2)')
    parser.add_argument('--parallel', type=int, default=1,
                        help='Anzahl paralleler Anfragen (Standard: 1, max empfohlen: 3)')

    args = parser.parse_args()

    # Megaprompt-Pfad
    prompt_pfad = args.prompt
    if not prompt_pfad:
        prompt_pfad = input("📝 Pfad zur Megaprompt DOCX-Datei: ").strip().strip('"')

    # Megaprompt laden
    print(f"\n📝 Lade Megaprompt: {prompt_pfad}")
    try:
        MEGAPROMPT_INHALT = lade_megaprompt(prompt_pfad)
        print(f"   ✓ Megaprompt geladen ({len(MEGAPROMPT_INHALT)} Zeichen)")
    except FileNotFoundError as e:
        print(f"❌ FEHLER: {e}")
        sys.exit(1)

    # API-Schlüssel prüfen
    api_schluessel = os.getenv('OPENAI_API_KEY')
    if not api_schluessel:
        print("❌ FEHLER: OPENAI_API_KEY nicht gefunden!")
        print("   Erstellen Sie eine .env-Datei mit:")
        print("   OPENAI_API_KEY=sk-ihr-api-schluessel")
        sys.exit(1)

    # PDF-Pfad
    pdf_pfad = args.pdf
    if not pdf_pfad:
        pdf_pfad = input("📁 Pfad zur PDF-Datei: ").strip().strip('"')

    if not os.path.exists(pdf_pfad):
        print(f"❌ FEHLER: Datei '{pdf_pfad}' nicht gefunden!")
        sys.exit(1)

    # Seitenanzahl
    gesamt_seiten = hole_pdf_seitenanzahl(pdf_pfad)
    print(f"\n📄 PDF: {pdf_pfad}")
    print(f"   Seiten: {gesamt_seiten}")

    # Bereich
    start_seite = args.start
    end_seite = args.end if args.end else gesamt_seiten

    if start_seite < 1:
        start_seite = 1
    if end_seite > gesamt_seiten:
        end_seite = gesamt_seiten
    if start_seite > end_seite:
        print(f"❌ FEHLER: Startseite ({start_seite}) > Endseite ({end_seite})")
        sys.exit(1)

    if args.test:
        end_seite = min(start_seite + 1, end_seite)
        print("🧪 TESTMODUS: max. 2 Seiten")

    anzahl_seiten = end_seite - start_seite + 1
    print(f"   Zu verarbeiten: Seiten {start_seite}-{end_seite} ({anzahl_seiten} Seiten)")
    print(f"   Modell: {args.model}")
    print(f"   Parallel: {args.parallel} Thread(s)")

    # Ausgabedatei
    if args.output:
        ausgabe_pfad = args.output
    else:
        pdf_name = Path(pdf_pfad).stem
        ausgabe_pfad = f"{pdf_name}_questions.csv"

    fehler_log_pfad = ausgabe_pfad.replace('.csv', '_errors.json')

    print(f"\n📁 Ausgabe: {ausgabe_pfad}")
    print(f"   Fehler-Log: {fehler_log_pfad}")

    # Buchseitennummer
    buch_start = args.book_start if args.book_start else (start_seite * 2 - 1)

    # OpenAI-Client
    client = OpenAI(api_key=api_schluessel)

    # Verbindungstest
    print("\n🔌 Teste Verbindung zur OpenAI API...")
    try:
        test_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Sage nur: OK"}],
            max_tokens=5,
            timeout=30
        )
        print("   ✓ Verbindung erfolgreich!")
    except Exception as e:
        print(f"   ❌ Verbindungsfehler: {type(e).__name__}")
        print(f"      {str(e)[:200]}")
        print("\n   Mögliche Ursachen:")
        print("   - Firewall blockiert api.openai.com")
        print("   - Proxy-Einstellungen erforderlich")
        print("   - VPN-Verbindung aktiv")
        print("   - API-Schlüssel ungültig")
        print("\n   Prüfen Sie: HTTPS_PROXY / HTTP_PROXY Umgebungsvariablen")
        antwort = input("\n   Trotzdem fortfahren? (j/n): ").strip().lower()
        if antwort != 'j':
            sys.exit(1)

    # Statistiken
    statistik = {
        "gesamt_seiten": anzahl_seiten,
        "erfolg": 0,
        "fehlgeschlagen": 0,
        "fragen_generiert": 0,
        "fehler_nach_typ": {}
    }

    # Aufgaben vorbereiten
    aufgaben = []
    for seiten_nr in range(start_seite - 1, end_seite):
        pdf_seite = seiten_nr + 1
        buch_seite_links = buch_start + (seiten_nr - (start_seite - 1)) * 2
        buch_seite_rechts = buch_seite_links + 1
        seiten_info = f"Buch S. {buch_seite_links}-{buch_seite_rechts}"

        aufgaben.append((
            client, pdf_pfad, seiten_nr, seiten_info,
            args.model, fehler_log_pfad, pdf_seite, gesamt_seiten
        ))

    # Kopfzeile schreiben
    datei_existiert = os.path.exists(ausgabe_pfad) and os.path.getsize(ausgabe_pfad) > 0

    with open(ausgabe_pfad, 'a', encoding='utf-8-sig', newline='') as csv_datei:
        if not datei_existiert:
            csv_datei.write(CSV_HEADER + '\n')
            print(f"\n✓ CSV-Kopfzeile geschrieben")

    print(f"\n{'='*60}")
    print("VERARBEITUNG GESTARTET")
    print(f"{'='*60}")

    fehler_liste = []
    startzeit = time.time()

    # Verarbeitung (sequentiell oder parallel)
    if args.parallel > 1:
        # Parallele Verarbeitung
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {executor.submit(verarbeite_seite_wrapper, aufgabe): aufgabe[6]
                      for aufgabe in aufgaben}

            for future in as_completed(futures):
                ergebnis = future.result()
                pdf_seite = ergebnis["pdf_seite"]
                seiten_info = ergebnis["seiten_info"]

                print(f"\n[Seite {pdf_seite}] {seiten_info}")

                if ergebnis["fehler"]:
                    statistik["fehlgeschlagen"] += 1
                    fehler_typ = ergebnis["fehler_typ"] or "UNBEKANNT"
                    statistik["fehler_nach_typ"][fehler_typ] = statistik["fehler_nach_typ"].get(fehler_typ, 0) + 1
                    fehler_liste.append(f"Seite {pdf_seite}: {ergebnis['fehler']}")
                    protokolliere_fehler(fehler_log_pfad, pdf_seite, seiten_info, fehler_typ, ergebnis["fehler"])
                    print(f"  ❌ {fehler_typ}: {ergebnis['fehler'][:50]}...")
                else:
                    statistik["erfolg"] += 1
                    statistik["fragen_generiert"] += ergebnis["anzahl_fragen"]

                    with schreib_lock:
                        with open(ausgabe_pfad, 'a', encoding='utf-8-sig', newline='') as csv_datei:
                            csv_datei.write(ergebnis["csv_inhalt"] + '\n')

                    print(f"  ✅ {ergebnis['anzahl_fragen']} Fragen generiert")
    else:
        # Sequentielle Verarbeitung
        for aufgabe in aufgaben:
            pdf_seite = aufgabe[6]
            seiten_info = aufgabe[3]

            print(f"\n{'='*60}")
            print(f"Seite {pdf_seite}/{gesamt_seiten} ({seiten_info})")
            print(f"{'='*60}")

            csv_inhalt, fehler, fehler_typ, anzahl = verarbeite_seite(
                client, pdf_pfad, aufgabe[2], seiten_info, args.model, fehler_log_pfad
            )

            if fehler:
                statistik["fehlgeschlagen"] += 1
                statistik["fehler_nach_typ"][fehler_typ] = statistik["fehler_nach_typ"].get(fehler_typ, 0) + 1
                fehler_liste.append(f"Seite {pdf_seite} ({seiten_info}): [{fehler_typ}] {fehler}")
                protokolliere_fehler(fehler_log_pfad, pdf_seite, seiten_info, fehler_typ, fehler)
                print(f"\n❌ FEHLGESCHLAGEN: {fehler_typ}")
                print(f"   {fehler}")
            else:
                statistik["erfolg"] += 1
                statistik["fragen_generiert"] += anzahl

                with open(ausgabe_pfad, 'a', encoding='utf-8-sig', newline='') as csv_datei:
                    csv_datei.write(csv_inhalt + '\n')

                print(f"\n✅ OK: {anzahl} Fragen generiert")

    # Zusammenfassung
    dauer = time.time() - startzeit

    print(f"\n{'='*60}")
    print("VERARBEITUNG ABGESCHLOSSEN")
    print(f"{'='*60}")

    print(f"\n📊 STATISTIK:")
    print(f"   Dauer: {dauer/60:.1f} Minuten ({dauer:.0f} Sekunden)")
    print(f"   Seiten verarbeitet: {statistik['gesamt_seiten']}")
    print(f"   Erfolgreich: {statistik['erfolg']} ({100*statistik['erfolg']/max(1,statistik['gesamt_seiten']):.1f}%)")
    print(f"   Fehlgeschlagen: {statistik['fehlgeschlagen']} ({100*statistik['fehlgeschlagen']/max(1,statistik['gesamt_seiten']):.1f}%)")
    print(f"   Fragen generiert: {statistik['fragen_generiert']}")
    if statistik['erfolg'] > 0:
        print(f"   Ø Fragen/Seite: {statistik['fragen_generiert']/statistik['erfolg']:.1f}")

    print(f"\n📁 DATEIEN:")
    print(f"   CSV: {ausgabe_pfad}")

    if fehler_liste:
        print(f"   Fehler-Log: {fehler_log_pfad}")
        print(f"\n⚠️  FEHLER NACH TYP:")
        for f_typ, anzahl in statistik["fehler_nach_typ"].items():
            print(f"   - {f_typ}: {anzahl} Seite(n)")
    else:
        print("\n✅ Keine Fehler!")


if __name__ == "__main__":
    main()
