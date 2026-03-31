#!/usr/bin/env python3
"""
Pipeline Elecciones Perú 2021: lectura PCM (CSV con `;` y campos finales vacíos),
métricas por departamento / provincia / distrito, ganadores, voto cruzado presidencial–congresal,
export SQLite + JSON.

Encoding: latin-1 / ISO-8859-1 (recomendado para datos ONPE).
Optimización: dtypes compactos y agregación por chunks en EG2021_Congresal.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
DATI = BASE / "dati"
OUT = BASE / "output"

CHUNK_LINES = 80_000


def read_csv_semicolon_fixed(path: Path, encoding: str = "latin-1") -> pd.DataFrame:
    """Evita desalineación cuando hay un campo vacío extra al final (``;;``)."""
    with open(path, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows: list[list[str]] = []
        for row in reader:
            if len(row) > len(header):
                row = row[: len(header)]
            elif len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            rows.append(row)
    return pd.DataFrame(rows, columns=header)


def find_2da_vuelta_path() -> Path:
    candidates = list(DATI.glob("Resultados_2da_vuelta*.csv"))
    if not candidates:
        raise FileNotFoundError("No se encontró Resultados_2da_vuelta*.csv en dati/")
    return candidates[0]


def load_dictionary() -> dict[str, Any]:
    with open(DATI / "diccionario_datos.json", encoding="utf-8") as f:
        return json.load(f)


def to_uint(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.replace("", "0"), errors="coerce").fillna(0).astype("uint32")


def prepare_presidencial(
    df: pd.DataFrame, pcols: list[str], contabilizada_only: bool = True
) -> pd.DataFrame:
    if contabilizada_only:
        df = df[df["DESCRIP_ESTADO_ACTA"] == "CONTABILIZADA"].copy()
    df["UBIGEO"] = df["UBIGEO"].astype(str).str.replace('"', "", regex=False).str.zfill(6)
    df["N_CVAS"] = to_uint(df["N_CVAS"])
    df["N_ELEC_HABIL"] = to_uint(df["N_ELEC_HABIL"])
    for c in pcols:
        df[c] = to_uint(df[c])
    for c in ("VOTOS_VB", "VOTOS_VN", "VOTOS_VI"):
        if c in df.columns:
            df[c] = to_uint(df[c])
    return df


def sum_votos_validos_pres(df: pd.DataFrame, pcols: list[str]) -> pd.Series:
    return df[pcols].sum(axis=1)


def metrics_block(
    emitidos: float,
    electores: float,
    votos_validos: float,
    vb: float,
    vn: float,
    vi: float,
) -> dict[str, float]:
    emitidos = max(emitidos, 1e-9)
    electores = max(electores, 1e-9)
    return {
        "electores_habiles": float(electores),
        "votos_emitidos": float(emitidos),
        "pct_participacion": float(emitidos / electores * 100.0),
        "pct_votos_validos_sobre_emitidos": float(votos_validos / emitidos * 100.0),
        "pct_votos_blancos_sobre_emitidos": float(vb / emitidos * 100.0),
        "pct_votos_nulos_sobre_emitidos": float(vn / emitidos * 100.0),
        "pct_votos_impugnados_sobre_emitidos": float(vi / emitidos * 100.0),
    }


def aggregate_pres_geo(
    df: pd.DataFrame,
    pcols: list[str],
    keys: list[str],
) -> pd.DataFrame:
    mesa = df.drop_duplicates(subset=["UBIGEO", "MESA_DE_VOTACION"])
    electores = mesa.groupby(keys, observed=True, sort=False)["N_ELEC_HABIL"].sum()

    g = df.groupby(keys, observed=True, sort=False)
    emitidos = g["N_CVAS"].sum()
    votos_validos = g[pcols].sum().sum(axis=1)
    sum_vb = g["VOTOS_VB"].sum() if "VOTOS_VB" in df.columns else 0
    sum_vn = g["VOTOS_VN"].sum() if "VOTOS_VN" in df.columns else 0
    sum_vi = g["VOTOS_VI"].sum() if "VOTOS_VI" in df.columns else 0

    out = pd.DataFrame(
        {
            "electores_habiles": electores,
            "votos_emitidos": emitidos,
            "votos_validos": votos_validos,
            "votos_blancos": sum_vb,
            "votos_nulos": sum_vn,
            "votos_impugnados": sum_vi,
        }
    )
    out["pct_participacion"] = out["votos_emitidos"] / out["electores_habiles"] * 100.0
    out["pct_validos_sobre_emitidos"] = out["votos_validos"] / out["votos_emitidos"] * 100.0
    out["pct_blancos_sobre_emitidos"] = out["votos_blancos"] / out["votos_emitidos"] * 100.0
    out["pct_nulos_sobre_emitidos"] = out["votos_nulos"] / out["votos_emitidos"] * 100.0
    out["pct_impugnados_sobre_emitidos"] = (
        out["votos_impugnados"] / out["votos_emitidos"] * 100.0
    )
    return out.reset_index()


def winner_table(
    df: pd.DataFrame,
    pcols: list[str],
    labels: dict[str, str],
    keys: list[str],
) -> pd.DataFrame:
    agg = df.groupby(keys, observed=True)[pcols].sum()
    arr = agg.to_numpy(dtype=np.float64)
    idx = arr.argmax(axis=1)
    winner_col = [pcols[i] for i in idx]
    top = arr[np.arange(arr.shape[0]), idx]
    sorted_row = np.sort(arr, axis=1)
    second = sorted_row[:, -2] if arr.shape[1] > 1 else np.zeros(arr.shape[0])
    margin = top - second
    margin_pct = np.divide(margin, np.maximum(top, 1.0)) * 100.0
    out = pd.DataFrame(
        {
            "ganador_columna": winner_col,
            "ganador_etiqueta": [labels.get(c, c) for c in winner_col],
            "votos_ganador": top.astype("int64"),
            "votos_segundo": second.astype("int64"),
            "margen_votos": margin.astype("int64"),
            "margen_pct_sobre_ganador": margin_pct,
        },
        index=agg.index,
    )
    return out.reset_index()


def iter_congresal_chunks(path: Path) -> Iterable[tuple[list[str], list[list[str]]]]:
    with open(path, encoding="latin-1") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        chunk: list[list[str]] = []
        for row in reader:
            if len(row) > len(header):
                row = row[: len(header)]
            elif len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            chunk.append(row)
            if len(chunk) >= CHUNK_LINES:
                yield header, chunk
                chunk = []
        if chunk:
            yield header, chunk


def process_congresal_chunk(
    header: list[str], rows: list[list[str]], nv_cols: list[str]
) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=header)
    df = df[df["DESCRIP_ESTADO_ACTA"] == "CONTABILIZADA"].copy()
    df["UBIGEO"] = df["UBIGEO"].astype(str).str.zfill(6)
    df["CODIGO_OP"] = (
        df["CODIGO_OP"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
    )
    for c in nv_cols:
        df[c] = pd.to_numeric(df[c].replace("", "0"), errors="coerce").fillna(0).astype("uint32")
    df["votos_partido"] = df[nv_cols].sum(axis=1)
    df = df[~df["CODIGO_OP"].isin(("00000080", "00000081", "00000082"))]
    return df[
        [
            "UBIGEO",
            "DEPARTAMENTO",
            "PROVINCIA",
            "DISTRITO",
            "CODIGO_OP",
            "DESCRIPCION_OP",
            "votos_partido",
        ]
    ]


def aggregate_congresal_by_ubigeo_codigo(path: Path) -> pd.DataFrame:
    nv_cols = [f"N_VOTOS_{i}" for i in range(1, 35)]
    totals: dict[tuple[str, str], int] = defaultdict(int)
    names: dict[str, str] = {}
    for header, chunk in iter_congresal_chunks(path):
        sub = process_congresal_chunk(header, chunk, nv_cols)
        if sub.empty:
            continue
        for cod, nom in zip(sub["CODIGO_OP"], sub["DESCRIPCION_OP"]):
            names[cod] = nom
        g = sub.groupby(["UBIGEO", "CODIGO_OP"], sort=False)["votos_partido"].sum()
        for (u, c), v in g.items():
            totals[(u, c)] += int(v)
    rows = [(u, c, v) for (u, c), v in totals.items()]
    out = pd.DataFrame(rows, columns=["UBIGEO", "CODIGO_OP", "votos"])
    out["DESCRIPCION_OP"] = out["CODIGO_OP"].map(names)
    return out


def build_sqlite(
    out_db: Path,
    dict_data: dict[str, Any],
    pres1: pd.DataFrame,
    pres2: pd.DataFrame,
    cong: pd.DataFrame,
    mapa_pres1_codigo: dict[str, str],
    mapa_pres2_codigo: dict[str, str],
) -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    if out_db.exists():
        out_db.unlink()

    meta = {
        "eleccion": dict_data.get("eleccion"),
        "criterio_actas": "Solo mesas con DESCRIP_ESTADO_ACTA = CONTABILIZADA",
        "nota_electores": (
            "La suma de N_ELEC_HABIL por mesa incluida puede diferir del padrón nacional ONPE "
            "si faltan mesas no contabilizadas o circunscripciones especiales."
        ),
        "fuentes": {
            "presidencial_1v": str(DATI / "Resultados_1ra_vuelta_Version_PCM.csv"),
            "presidencial_2v": str(find_2da_vuelta_path()),
            "congresal": str(DATI / "EG2021_Congresal.csv"),
        },
        "diccionario": str(DATI / "diccionario_datos.json"),
    }

    conn = sqlite3.connect(out_db)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE meta (clave TEXT PRIMARY KEY, valor TEXT)"
    )
    cur.execute(
        "INSERT INTO meta VALUES (?, ?)",
        ("json", json.dumps(meta, ensure_ascii=False)),
    )

    p1cols = [c for c in pres1.columns if c.startswith("VOTOS_P")]
    p2cols = [c for c in pres2.columns if c.startswith("VOTOS_P")]
    labels1 = {
        k: v["nombre"] + " — " + v["partido"]
        for k, v in dict_data["presidencial_1vuelta"]["candidatos"].items()
    }
    labels2 = {
        k: v["nombre"] + " — " + v["partido"]
        for k, v in dict_data["presidencial_2vuelta"]["candidatos"].items()
    }

    for name, df, pcols, lbl, tag in (
        ("metricas_pres1_departamento", pres1, p1cols, labels1, "1v"),
        ("metricas_pres1_provincia", pres1, p1cols, labels1, "1v"),
        ("metricas_pres1_distrito", pres1, p1cols, labels1, "1v"),
        ("metricas_pres2_departamento", pres2, p2cols, labels2, "2v"),
        ("metricas_pres2_provincia", pres2, p2cols, labels2, "2v"),
        ("metricas_pres2_distrito", pres2, p2cols, labels2, "2v"),
    ):
        if "provincia" in name:
            keys = ["DEPARTAMENTO", "PROVINCIA"]
        elif "distrito" in name:
            keys = ["UBIGEO", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"]
        else:
            keys = ["DEPARTAMENTO"]
        m = aggregate_pres_geo(df, pcols, keys)
        m.to_sql(name, conn, index=False, if_exists="replace")

    for name, df, pcols, lbl in (
        ("ganadores_pres1_departamento", pres1, p1cols, labels1),
        ("ganadores_pres1_provincia", pres1, p1cols, labels1),
        ("ganadores_pres1_distrito", pres1, p1cols, labels1),
        ("ganadores_pres2_departamento", pres2, p2cols, labels2),
        ("ganadores_pres2_provincia", pres2, p2cols, labels2),
        ("ganadores_pres2_distrito", pres2, p2cols, labels2),
    ):
        if "provincia" in name:
            keys = ["DEPARTAMENTO", "PROVINCIA"]
        elif "distrito" in name:
            keys = ["UBIGEO", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"]
        else:
            keys = ["DEPARTAMENTO"]
        w = winner_table(df, pcols, lbl, keys)
        w.to_sql(name, conn, index=False, if_exists="replace")

    cong.to_sql("congreso_votos_por_distrito_partido", conn, index=False, if_exists="replace")

    s1 = pres1[p1cols].sum()
    pd.DataFrame(
        {
            "columna": p1cols,
            "votos": [int(s1[c]) for c in p1cols],
            "etiqueta": [labels1[c] for c in p1cols],
        }
    ).sort_values("votos", ascending=False).to_sql(
        "pres1_votos_candidato_nacional", conn, index=False, if_exists="replace"
    )
    s2 = pres2[p2cols].sum()
    pd.DataFrame(
        {
            "columna": p2cols,
            "votos": [int(s2[c]) for c in p2cols],
            "etiqueta": [labels2[c] for c in p2cols],
        }
    ).sort_values("votos", ascending=False).to_sql(
        "pres2_votos_candidato_nacional", conn, index=False, if_exists="replace"
    )
    cong.groupby(["CODIGO_OP", "DESCRIPCION_OP"], as_index=False)["votos"].sum().sort_values(
        "votos", ascending=False
    ).to_sql("congreso_votos_partido_nacional", conn, index=False, if_exists="replace")

    gcong = cong.sort_values("votos", ascending=False).groupby("UBIGEO", sort=False).head(1)
    gcong = gcong.rename(
        columns={
            "CODIGO_OP": "congreso_codigo_ganador",
            "DESCRIPCION_OP": "congreso_partido_ganador",
            "votos": "congreso_votos_ganador",
        }
    )

    w1 = winner_table(pres1, p1cols, labels1, ["UBIGEO", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"])
    w1 = w1.rename(
        columns={
            "ganador_columna": "pres1_columna_ganador",
            "ganador_etiqueta": "pres1_ganador",
            "votos_ganador": "pres1_votos_ganador",
        }
    )
    w1["pres1_codigo_partido_mapeado"] = w1["pres1_columna_ganador"].map(mapa_pres1_codigo)

    cruz = w1.merge(
        gcong[["UBIGEO", "congreso_codigo_ganador", "congreso_partido_ganador", "congreso_votos_ganador"]],
        on="UBIGEO",
        how="inner",
    )
    cruz["misma_bancada_pres1_congreso"] = (
        cruz["pres1_codigo_partido_mapeado"] == cruz["congreso_codigo_ganador"]
    )
    cruz.to_sql("voto_cruzado_distrito", conn, index=False, if_exists="replace")

    w2 = winner_table(pres2, p2cols, labels2, ["UBIGEO", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"])
    w2 = w2.rename(
        columns={
            "ganador_columna": "pres2_columna_ganador",
            "ganador_etiqueta": "pres2_ganador",
        }
    )
    w2["pres2_codigo_partido_mapeado"] = w2["pres2_columna_ganador"].map(mapa_pres2_codigo)
    cruz2 = w2.merge(
        gcong[["UBIGEO", "congreso_codigo_ganador", "congreso_partido_ganador"]],
        on="UBIGEO",
        how="inner",
    )
    cruz2["misma_bancada_pres2_congreso"] = (
        cruz2["pres2_codigo_partido_mapeado"] == cruz2["congreso_codigo_ganador"]
    )
    cruz2.to_sql("voto_cruzado_distrito_2v", conn, index=False, if_exists="replace")

    conn.commit()
    conn.close()


def normalize_departamento(s: str) -> str:
    s = s.strip().upper()
    for a, b in (
        ("Á", "A"),
        ("É", "E"),
        ("Í", "I"),
        ("Ó", "O"),
        ("Ú", "U"),
        ("Ñ", "N"),
    ):
        s = s.replace(a, b)
    return s


def load_espectro() -> dict[str, Any]:
    with open(DATI / "espectro_politico_2021.json", encoding="utf-8") as f:
        return json.load(f)


def load_geo_peru_light() -> dict[str, Any]:
    path = DATI / "geo" / "peru_departamentos.geojson"
    with open(path, encoding="utf-8") as f:
        g = json.load(f)
    for feat in g.get("features", []):
        nom = feat.get("properties", {}).get("NOMBDEP", "")
        feat["properties"] = {"NOMBDEP": nom}
    return g


def totales_bloques_pres1_nacional(pres1: pd.DataFrame, pcols: list[str], esp: dict[str, Any]) -> dict[str, float]:
    cmap = esp["candidatos_pres1"]
    tot = {"izquierda": 0, "centro": 0, "derecha": 0}
    for c in pcols:
        b = cmap.get(c, "centro")
        tot[b] += int(pres1[c].sum())
    vv = sum(tot.values())
    if not vv:
        return {k: 0.0 for k in tot}
    return {k: 100.0 * tot[k] / vv for k in tot}


def totales_bloques_congreso_nacional(cong: pd.DataFrame, esp: dict[str, Any]) -> dict[str, float]:
    pmap = esp["partidos_congreso"]
    tot = {"izquierda": 0, "centro": 0, "derecha": 0}
    for _, r in cong.iterrows():
        cod = str(r["CODIGO_OP"]).zfill(8)
        b = pmap.get(cod, "centro")
        tot[b] += int(r["votos"])
    vv = sum(tot.values())
    if not vv:
        return {k: 0.0 for k in tot}
    return {k: 100.0 * tot[k] / vv for k in tot}


def bloques_pres1_por_departamento(
    pres1: pd.DataFrame, pcols: list[str], esp: dict[str, Any]
) -> list[dict[str, Any]]:
    cmap = esp["candidatos_pres1"]
    out: list[dict[str, Any]] = []
    for dep, g in pres1.groupby("DEPARTAMENTO", sort=False):
        tot = {"izquierda": 0, "centro": 0, "derecha": 0}
        for c in pcols:
            b = cmap.get(c, "centro")
            tot[b] += int(g[c].sum())
        vv = sum(tot.values())
        if vv == 0:
            continue
        out.append(
            {
                "DEPARTAMENTO": dep,
                "pct_izquierda": round(100.0 * tot["izquierda"] / vv, 3),
                "pct_centro": round(100.0 * tot["centro"] / vv, 3),
                "pct_derecha": round(100.0 * tot["derecha"] / vv, 3),
                "balance_izq_menos_der_pp": round(
                    100.0 * (tot["izquierda"] - tot["derecha"]) / vv, 3
                ),
            }
        )
    return out


def castillo_por_departamento(pres2: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for dep, g in pres2.groupby("DEPARTAMENTO", sort=False):
        v1 = int(g["VOTOS_P1"].sum())
        v2 = int(g["VOTOS_P2"].sum())
        vv = v1 + v2
        out.append(
            {
                "DEPARTAMENTO": dep,
                "pct_castillo": round(100.0 * v1 / vv, 3) if vv else 0.0,
                "pct_fujimori": round(100.0 * v2 / vv, 3) if vv else 0.0,
            }
        )
    return out


def bloques_congreso_por_departamento(cong: pd.DataFrame, esp: dict[str, Any]) -> list[dict[str, Any]]:
    pmap = esp["partidos_congreso"]
    out: list[dict[str, Any]] = []
    for dep, g in cong.groupby("DEPARTAMENTO", sort=False):
        tot = {"izquierda": 0, "centro": 0, "derecha": 0}
        for _, r in g.iterrows():
            cod = str(r["CODIGO_OP"]).zfill(8)
            b = pmap.get(cod, "centro")
            tot[b] += int(r["votos"])
        vv = sum(tot.values())
        if vv == 0:
            continue
        out.append(
            {
                "DEPARTAMENTO": dep,
                "pct_izquierda": round(100.0 * tot["izquierda"] / vv, 3),
                "pct_centro": round(100.0 * tot["centro"] / vv, 3),
                "pct_derecha": round(100.0 * tot["derecha"] / vv, 3),
            }
        )
    return out


def series_mapa_desde_metricas(
    filas: list[dict[str, Any]], campo_valor: str, geo: dict[str, Any]
) -> list[dict[str, Any]]:
    by_n = {normalize_departamento(r["DEPARTAMENTO"]): r[campo_valor] for r in filas}
    series: list[dict[str, Any]] = []
    for feat in geo.get("features", []):
        nom = feat["properties"]["NOMBDEP"]
        val = by_n.get(normalize_departamento(nom))
        series.append({"name": nom, "value": round(float(val), 2) if val is not None else 0.0})
    return series


def export_dashboard_json(
    db_path: Path,
    out_path: Path,
    resumen_nacional: dict[str, Any],
    pres1: pd.DataFrame,
    pres2: pd.DataFrame,
    cong: pd.DataFrame,
    p1cols: list[str],
    p2cols: list[str],
) -> dict[str, Any]:
    """Genera JSON per la dashboard HTML (Apache ECharts)."""
    conn = sqlite3.connect(db_path)
    m1 = pd.read_sql_query("SELECT * FROM metricas_pres1_departamento", conn)
    m2 = pd.read_sql_query("SELECT * FROM metricas_pres2_departamento", conn)
    g1 = pd.read_sql_query(
        "SELECT DEPARTAMENTO, ganador_etiqueta AS ganador_1v, "
        "margen_pct_sobre_ganador AS margen_pct_1v FROM ganadores_pres1_departamento",
        conn,
    )
    g2 = pd.read_sql_query(
        "SELECT DEPARTAMENTO, ganador_etiqueta AS ganador_2v, "
        "margen_pct_sobre_ganador AS margen_pct_2v FROM ganadores_pres2_departamento",
        conn,
    )
    cruz = pd.read_sql_query(
        "SELECT misma_bancada_pres1_congreso FROM voto_cruzado_distrito", conn
    )
    pres1_cand = pd.read_sql_query(
        "SELECT * FROM pres1_votos_candidato_nacional", conn
    ).to_dict("records")
    pres2_cand = pd.read_sql_query(
        "SELECT * FROM pres2_votos_candidato_nacional", conn
    ).to_dict("records")
    cong_nat = pd.read_sql_query(
        "SELECT * FROM congreso_votos_partido_nacional LIMIT 22", conn
    ).to_dict("records")
    provincias = pd.read_sql_query(
        """
        SELECT DEPARTAMENTO, PROVINCIA, electores_habiles, votos_emitidos, pct_participacion,
               pct_validos_sobre_emitidos
        FROM metricas_pres1_provincia
        ORDER BY electores_habiles DESC
        LIMIT 36
        """,
        conn,
    ).to_dict("records")
    dist_hi = pd.read_sql_query(
        """
        SELECT UBIGEO, DEPARTAMENTO, PROVINCIA, DISTRITO, pct_participacion, electores_habiles
        FROM metricas_pres1_distrito
        WHERE UBIGEO NOT LIKE '9%'
        ORDER BY pct_participacion DESC
        LIMIT 14
        """,
        conn,
    ).to_dict("records")
    dist_lo = pd.read_sql_query(
        """
        SELECT UBIGEO, DEPARTAMENTO, PROVINCIA, DISTRITO, pct_participacion, electores_habiles
        FROM metricas_pres1_distrito
        WHERE UBIGEO NOT LIKE '9%' AND electores_habiles >= 50
        ORDER BY pct_participacion ASC
        LIMIT 14
        """,
        conn,
    ).to_dict("records")
    conn.close()

    merged = (
        m1.merge(m2, on="DEPARTAMENTO", suffixes=("_1v", "_2v"))
        .merge(g1, on="DEPARTAMENTO")
        .merge(g2, on="DEPARTAMENTO")
    )

    dept_rows = []
    for _, row in merged.iterrows():
        dept_rows.append(
            {
                "departamento": row["DEPARTAMENTO"],
                "pres1": {
                    "pct_participacion": float(row["pct_participacion_1v"]),
                    "pct_validos": float(row["pct_validos_sobre_emitidos_1v"]),
                    "pct_blancos": float(row["pct_blancos_sobre_emitidos_1v"]),
                    "pct_nulos": float(row["pct_nulos_sobre_emitidos_1v"]),
                    "electores": int(row["electores_habiles_1v"]),
                    "emitidos": int(row["votos_emitidos_1v"]),
                    "ganador": str(row["ganador_1v"]),
                    "margen_pct": float(row["margen_pct_1v"]),
                },
                "pres2": {
                    "pct_participacion": float(row["pct_participacion_2v"]),
                    "pct_validos": float(row["pct_validos_sobre_emitidos_2v"]),
                    "pct_blancos": float(row["pct_blancos_sobre_emitidos_2v"]),
                    "pct_nulos": float(row["pct_nulos_sobre_emitidos_2v"]),
                    "electores": int(row["electores_habiles_2v"]),
                    "emitidos": int(row["votos_emitidos_2v"]),
                    "ganador": str(row["ganador_2v"]),
                    "margen_pct": float(row["margen_pct_2v"]),
                },
                "delta_participacion_2v_menos_1v": float(
                    row["pct_participacion_2v"] - row["pct_participacion_1v"]
                ),
            }
        )

    esp = load_espectro()
    geo_peru = load_geo_peru_light()
    bl_pres1_dep = bloques_pres1_por_departamento(pres1, p1cols, esp)
    bl_cast_dep = castillo_por_departamento(pres2)
    bl_cong_dep = bloques_congreso_por_departamento(cong, esp)
    map_pres1_bal = series_mapa_desde_metricas(
        bl_pres1_dep, "balance_izq_menos_der_pp", geo_peru
    )
    map_pres2_cas = series_mapa_desde_metricas(bl_cast_dep, "pct_castillo", geo_peru)

    aligned = int(cruz["misma_bancada_pres1_congreso"].sum())
    total_d = len(cruz)
    bundle = {
        "fuente": "https://www.datosabiertos.gob.pe/",
        "demo": False,
        "resumen": resumen_nacional,
        "espectro_nota": esp.get("nota", ""),
        "nacional_bloques_pres1": totales_bloques_pres1_nacional(pres1, p1cols, esp),
        "nacional_bloques_congreso": totales_bloques_congreso_nacional(cong, esp),
        "departamentos_bloques_pres1": bl_pres1_dep,
        "departamentos_castillo_2v": bl_cast_dep,
        "departamentos_bloques_congreso": bl_cong_dep,
        "geo_peru": geo_peru,
        "mapa_pres1_balance_izq_der": map_pres1_bal,
        "mapa_pres2_pct_castillo": map_pres2_cas,
        "voto_cruzado_distrito": {
            "distritos_total": total_d,
            "misma_bancada_1v_congreso": aligned,
            "distinto": total_d - aligned,
            "pct_misma_bancada": round(100.0 * aligned / max(total_d, 1), 2),
        },
        "departamentos": sorted(dept_rows, key=lambda x: x["departamento"]),
        "line3d_pres_participacion": [
            [i, row["pres1"]["pct_participacion"], row["pres2"]["pct_participacion"]]
            for i, row in enumerate(sorted(dept_rows, key=lambda x: x["departamento"]))
        ],
        "pres1_candidatos_nacional": pres1_cand,
        "pres2_candidatos_nacional": pres2_cand,
        "congreso_partidos_nacional": cong_nat,
        "provincias_mayor_electores": provincias,
        "distritos_alta_participacion_1v": dist_hi,
        "distritos_baja_participacion_1v": dist_lo,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)
    return bundle


def json_embed_script(obj: Any) -> str:
    """Serializa JSON seguro dentro de <script type=\"application/json\">."""
    return json.dumps(obj, ensure_ascii=False).replace("<", "\\u003c")


def write_dashboard_html(bundle: dict[str, Any], template_path: Path, out_path: Path) -> None:
    text = template_path.read_text(encoding="utf-8")
    replacement = json_embed_script(bundle)
    pattern = re.compile(
        r'(<script\s+type="application/json"\s+id="bundle"\s*>)([\s\S]*?)(</script>)',
        re.IGNORECASE,
    )
    if not pattern.search(text):
        raise ValueError(
            f'No se encontró <script type="application/json" id="bundle"> en {template_path}'
        )
    out = pattern.sub(lambda m: m.group(1) + replacement + m.group(3), text, count=1)
    out_path.write_text(out, encoding="utf-8")


def nacional_metrics(
    df: pd.DataFrame, pcols: list[str], label: str
) -> dict[str, Any]:
    df = prepare_presidencial(df, pcols, contabilizada_only=True)
    mesa = df.drop_duplicates(subset=["UBIGEO", "MESA_DE_VOTACION"])
    electores = int(mesa["N_ELEC_HABIL"].sum())
    emitidos = int(df["N_CVAS"].sum())
    vv = int(sum_votos_validos_pres(df, pcols).sum())
    vb = int(df["VOTOS_VB"].sum()) if "VOTOS_VB" in df.columns else 0
    vn = int(df["VOTOS_VN"].sum()) if "VOTOS_VN" in df.columns else 0
    vi = int(df["VOTOS_VI"].sum()) if "VOTOS_VI" in df.columns else 0
    return {
        "nivel": "nacional",
        "turno": label,
        **metrics_block(emitidos, electores, vv, vb, vn, vi),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-db",
        type=Path,
        default=OUT / "elecciones_2021.sqlite",
        help="Ruta de salida SQLite",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=OUT / "resumen_nacional.json",
        help="Resumen JSON nacional",
    )
    parser.add_argument(
        "--dashboard-json",
        type=Path,
        default=BASE / "dashboard" / "data.json",
        help="JSON bundle para la vista HTML (Apache ECharts)",
    )
    parser.add_argument(
        "--dashboard-html",
        type=Path,
        default=BASE / "dashboard" / "index.html",
        help="HTML autónomo (datos incrustados)",
    )
    args = parser.parse_args()

    d = load_dictionary()
    p1cols = list(d["presidencial_1vuelta"]["candidatos"].keys())
    p2cols = list(d["presidencial_2vuelta"]["candidatos"].keys())

    mapa_pres1_codigo = {
        k: v["codigo_op_congreso"] for k, v in d["presidencial_1vuelta"]["candidatos"].items()
    }
    mapa_pres2_codigo = {
        k: v["codigo_op_congreso"] for k, v in d["presidencial_2vuelta"]["candidatos"].items()
    }

    pres1 = prepare_presidencial(
        read_csv_semicolon_fixed(DATI / "Resultados_1ra_vuelta_Version_PCM.csv"), p1cols
    )
    pres2 = prepare_presidencial(read_csv_semicolon_fixed(find_2da_vuelta_path()), p2cols)

    print("Agregando congreso por distrito (chunks)...")
    cong = aggregate_congresal_by_ubigeo_codigo(DATI / "EG2021_Congresal.csv")
    geo = pres1[["UBIGEO", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"]].drop_duplicates(
        subset=["UBIGEO"]
    )
    cong = cong.merge(geo, on="UBIGEO", how="left")

    build_sqlite(
        args.out_db,
        d,
        pres1,
        pres2,
        cong,
        mapa_pres1_codigo,
        mapa_pres2_codigo,
    )

    resumen = {
        "nacional": {
            "presidencial_1v": nacional_metrics(
                read_csv_semicolon_fixed(DATI / "Resultados_1ra_vuelta_Version_PCM.csv"),
                p1cols,
                "1v",
            ),
            "presidencial_2v": nacional_metrics(
                read_csv_semicolon_fixed(find_2da_vuelta_path()), p2cols, "2v"
            ),
        },
        "metodologia": {
            "encoding_csv": "latin-1 (ISO-8859-1)",
            "mesas": "Solo CONTABILIZADA",
            "congreso": "Suma de N_VOTOS_1..34 por fila de partido; excluye filas 80/81/82 (blanco/nulo/impugnado).",
        },
        "voto_cruzado": {
            "definicion": (
                "Por distrito (UBIGEO): comparación ecológica entre el partido del candidato "
                "presidencial más votado y el partido más votado en la cédula congresal. "
                "No es voto individual (los datos son agregados por mesa)."
            )
        },
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)
    bundle = export_dashboard_json(
        args.out_db,
        args.dashboard_json,
        resumen,
        pres1,
        pres2,
        cong,
        p1cols,
        p2cols,
    )
    tpl = BASE / "dashboard" / "index.template.html"
    write_dashboard_html(bundle, tpl, args.dashboard_html)
    print(f"SQLite: {args.out_db}")
    print(f"JSON:   {args.out_json}")
    print(f"Dashboard: {args.dashboard_json} + {args.dashboard_html}")


if __name__ == "__main__":
    main()
