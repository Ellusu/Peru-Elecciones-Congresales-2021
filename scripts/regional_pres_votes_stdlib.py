#!/usr/bin/env python3
"""Calcula pres{1,2}_votos_por_departamento sin pandas (solo stdlib). Usado si el build principal no está disponible."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DATI = BASE / "dati"
DASH = BASE / "dashboard"


def _fix_row(row: list[str], n: int) -> list[str]:
    if len(row) > n:
        row = row[:n]
    elif len(row) < n:
        row = row + [""] * (n - len(row))
    return row


def read_csv_semicolon_fixed(path: Path, encoding: str = "latin-1") -> list[dict[str, str]]:
    with open(path, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows: list[dict[str, str]] = []
        for row in reader:
            row = _fix_row(row, len(header))
            rows.append(dict(zip(header, row)))
    return rows


def labels_from_dict(d: dict, turn: str) -> dict[str, str]:
    key = "presidencial_1vuelta" if turn == "1v" else "presidencial_2vuelta"
    return {
        k: v["nombre"] + " — " + v["partido"]
        for k, v in d[key]["candidatos"].items()
    }


def pres_votos_por_departamento(
    rows: list[dict[str, str]], pcols: list[str], labels: dict[str, str]
) -> list[dict]:
    dep_votes: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        if row.get("DESCRIP_ESTADO_ACTA", "").strip() != "CONTABILIZADA":
            continue
        dep = (row.get("DEPARTAMENTO") or "").strip().strip('"')
        if not dep:
            continue
        for c in pcols:
            raw = (row.get(c) or "").strip().strip('"')
            if not raw:
                v = 0
            else:
                try:
                    v = int(raw)
                except ValueError:
                    v = int(float(raw))
            dep_votes[dep][c] += v
    out: list[dict] = []
    for dep in sorted(dep_votes.keys()):
        tot = dep_votes[dep]
        cand_sorted = sorted(
            (
                {"columna": c, "etiqueta": labels.get(c, c), "votos": tot[c]}
                for c in pcols
            ),
            key=lambda x: -x["votos"],
        )
        out.append({"departamento": dep, "candidatos": cand_sorted})
    return out


def find_2da_vuelta_path() -> Path:
    candidates = list(DATI.glob("Resultados_2da_vuelta*.csv"))
    if not candidates:
        raise FileNotFoundError("No se encontró Resultados_2da_vuelta*.csv en dati/")
    return candidates[0]


def main() -> None:
    d = json.loads((DATI / "diccionario_datos.json").read_text(encoding="utf-8"))
    p1cols = list(d["presidencial_1vuelta"]["candidatos"].keys())
    p2cols = list(d["presidencial_2vuelta"]["candidatos"].keys())
    lbl1 = labels_from_dict(d, "1v")
    lbl2 = labels_from_dict(d, "2v")

    r1 = read_csv_semicolon_fixed(DATI / "Resultados_1ra_vuelta_Version_PCM.csv")
    r2 = read_csv_semicolon_fixed(find_2da_vuelta_path())

    pres1_dep = pres_votos_por_departamento(r1, p1cols, lbl1)
    pres2_dep = pres_votos_por_departamento(r2, p2cols, lbl2)

    # dashboard/data.json
    dj_path = DASH / "data.json"
    if dj_path.is_file():
        bundle = json.loads(dj_path.read_text(encoding="utf-8"))
        bundle["pres1_votos_por_departamento"] = pres1_dep
        bundle["pres2_votos_por_departamento"] = pres2_dep
        dj_path.write_text(
            json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Updated {dj_path}")

    # index.html (bundle embebido)
    html_path = DASH / "index.html"
    if html_path.is_file():
        text = html_path.read_text(encoding="utf-8")
        pat = re.compile(
            r'(<script\s+type="application/json"\s+id="bundle"\s*>)([\s\S]*?)(</script>)',
            re.IGNORECASE,
        )
        m = pat.search(text)
        if not m:
            raise SystemExit("No bundle in index.html")
        bundle = json.loads(m.group(2))
        bundle["pres1_votos_por_departamento"] = pres1_dep
        bundle["pres2_votos_por_departamento"] = pres2_dep
        repl = json.dumps(bundle, ensure_ascii=False).replace("<", "\\u003c")
        new_html = pat.sub(lambda x: x.group(1) + repl + x.group(3), text, count=1)
        html_path.write_text(new_html, encoding="utf-8")
        print(f"Updated {html_path}")


if __name__ == "__main__":
    main()
