# PDF Exam Question Generator — AI-Powered IHK Training Tool

> Automatically transforms vocational training textbooks (PDF) into structured multiple-choice exam questions using OpenAI Vision — ready for import into Excel/LMS.

---

## What it does

This tool reads any PDF textbook page by page, sends each page as an image to OpenAI (GPT-4.1 / GPT-5.1 / GPT-5.2), and generates IHK-style single-choice exam questions in a structured CSV format.

**Input:** Any scanned or OCR'd vocational textbook (PDF)
**Output:** A ready-to-use `.csv` file with 12 columns per question

```
Frage ; A ; B ; C ; D ; Richtig ; Richtig_Text ; Thema ; Quelle ; Status ; Kommentar ; Vollansicht
```

---

## Key Features

- **Multi-model support** — GPT-4.1, GPT-5.1, GPT-5.2
- **Parallel processing** — up to N simultaneous API requests for speed
- **Smart page detection** — auto-detects image-heavy vs. text-heavy pages and adapts the prompt strategy
- **Graceful interruption** — `Ctrl+C` saves all progress, resume from where you stopped
- **Flexible page mapping** — single-page or double-page scan mode, custom book page offset
- **Per-book megaprompts** — each book can have its own custom prompt (vocabulary, topics, style)
- **Auto retry & backoff** — handles rate limits, timeouts and connection errors automatically

---

## Usage

```bash
# Basic
python pdf_to_csv.py --pdf Buch.pdf --prompt Megaprompt.docx

# Full example
python pdf_to_csv.py \
  --pdf Friseur.pdf \
  --prompt Megaprompt_Friseur.docx \
  --model gpt-5.1 \
  --start 11 \
  --end 406 \
  --parallel 5 \
  --single-page \
  --book-start 1
```

### Arguments

| Argument | Description |
|---|---|
| `--pdf` | Path to the PDF file |
| `--prompt` | Path to the megaprompt `.docx` file |
| `--model` | `gpt-4.1`, `gpt-5.1`, `gpt-5.2` (default: `gpt-4.1`) |
| `--start` | First PDF page to process (default: 1) |
| `--end` | Last PDF page to process (default: last) |
| `--parallel` | Number of parallel API requests (default: 3) |
| `--single-page` | 1 PDF page = 1 book page (default: double-page mode) |
| `--book-start` | Book page number of the first processed PDF page |
| `--test` | Process only 2 pages (for testing) |

---

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file:
```
OPENAI_API_KEY=sk-your-key-here
```

---

## Tech Stack

- **Python 3.10+**
- **PyMuPDF** — PDF rendering
- **OpenAI Python SDK** — Vision API
- **python-docx** — Megaprompt loading
- **ThreadPoolExecutor** — Parallel processing

---

## Vocational Sectors Already Tested

Retail · Office Management · Automotive · Banking · Real Estate · Logistics · Construction · Hairdressing · Painting · Dental · Emergency Medical Services

---

## License

MIT
