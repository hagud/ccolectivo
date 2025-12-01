#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explorador HTML de ejemplos — AUTO-FINDER por fases
---------------------------------------------------

Este script detecta AUTOMÁTICAMENTE todos los JSON de clusters en tests_output/
y genera un explorador de ejemplos para CADA FASE (archivo .json).

Estructura de salida:

explorador_html/
    index.html                  ← índice global de fases
    fases/
        01_strong_only/
            index.html
            duplicados/
                LIB1.html
                LIB2.html
                ...
            interbibliotecas/
                LIB1+LIB2.html
                ...
            sospechosos/
                top_50.html
                cluster_<id>.html
            heatmap/
                heatmap.html

        02_fuzzy_safe/
            ...

        03_fuzzy_standard/
        04_fuzzy_aggressive/
        10_fuzzy_marc_97/
        11_fuzzy_marc_98/
        12_fuzzy_marc_99/
        ...

Uso:

    python3 explorador_autofinder.py generar

Requisitos:
  - JSON de clusters generados por colectivo_v6_mini.py en:
        ./tests_output/*.json
"""

import os
import json
import argparse
from typing import List, Dict, Tuple

# -------------------------------------------------------------------
# RUTAS BASE
# -------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(BASE_DIR, "tests_output")
OUT_DIR = os.path.join(BASE_DIR, "explorador_html")


# -------------------------------------------------------------------
# UTILIDADES HTML
# -------------------------------------------------------------------

def esc(s: str) -> str:
    """Escape básico HTML."""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def write_html(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def html_page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>{esc(title)}</title>
<style>
body {{
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    margin: 0;
    background: #f5f7fa;
}}
header {{
    background: white;
    border-bottom: 1px solid #d0d7e2;
    padding: 12px 20px;
}}
header h1 {{
    margin: 0;
    font-size: 20px;
}}
header small {{
    font-size: 12px;
    color: #6b7280;
}}
main {{
    padding: 20px;
}}
.card {{
    background: white;
    border: 1px solid #d0d7e2;
    border-radius: 10px;
    padding: 16px;
    margin-bottom: 18px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 10px;
    font-size: 13px;
}}
td, th {{
    border: 1px solid #d0d7e2;
    padding: 4px;
}}
th {{
    background: #eef2f7;
}}
a {{
    color: #2563eb;
    text-decoration: none;
}}
.small {{
    font-size: 12px;
    color: #6b7280;
}}
</style>
</head>
<body>

<header>
  <h1>{esc(title)}</h1>
  <small>Explorador de ejemplos por fase</small>
</header>

<main>
{body}
</main>

</body>
</html>
"""


# -------------------------------------------------------------------
# CARGA DE FASES Y CLUSTERS
# -------------------------------------------------------------------

def detect_phases() -> List[str]:
    """Detecta todas las fases a partir de los .json en tests_output."""
    if not os.path.isdir(TESTS_DIR):
        print(f"ERROR: no existe la carpeta {TESTS_DIR}")
        return []

    phases = []
    for name in os.listdir(TESTS_DIR):
        if not name.endswith(".json"):
            continue
        if name.startswith("."):
            continue
        phase = name[:-5]  # quitar .json
        phases.append(phase)

    phases.sort()
    return phases


def load_clusters_for_phase(phase: str) -> List[dict]:
    """Carga el JSON de clusters de una fase concreta."""
    path = os.path.join(TESTS_DIR, f"{phase}.json")
    if not os.path.exists(path):
        print(f"  [!] No existe {path}, fase ignorada.")
        return []
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        print(f"  [!] {path} no parece un listado de clusters (no es lista).")
        return []
    return data


# -------------------------------------------------------------------
# BLOQUE: DUPLICACIÓN INTERNA
# -------------------------------------------------------------------

def build_duplicados_for_phase(phase: str, data: List[dict], phase_dir: str) -> List[str]:
    """
    Genera los HTML de duplicados internos para una fase concreta.
    Devuelve las filas HTML para el índice de la fase.
    """
    folder = os.path.join(phase_dir, "duplicados")
    os.makedirs(folder, exist_ok=True)

    dup_by_lib: Dict[str, List[dict]] = {}

    for c in data:
        cid = c.get("cluster_id")
        members = c.get("members", [])
        if not isinstance(members, list):
            continue
        size = len(members)
        if size <= 1:
            continue
        libs = [m.get("lib") for m in members if m.get("lib") is not None]
        if not libs:
            continue
        uniq = set(libs)
        if len(uniq) == 1:
            lib = list(uniq)[0]
            dup_by_lib.setdefault(lib, []).append(c)

    index_rows = []

    for lib, clusters in sorted(dup_by_lib.items()):
        page = os.path.join(folder, f"{lib}.html")

        rows = []
        for c in clusters:
            cid = c.get("cluster_id")
            members = c.get("members", [])
            ids = ", ".join(esc(m.get("local_id")) for m in members)
            rows.append(
                f"<tr>"
                f"<td>{cid}</td>"
                f"<td>{len(members)}</td>"
                f"<td>{ids}</td>"
                f"<td><a href='../sospechosos/cluster_{cid}.html'>Ver cluster</a></td>"
                f"</tr>"
            )

        body = f"<h2>Duplicación interna en {esc(lib)} — Fase {esc(phase)}</h2>"
        body += "<p class='small'>Ejemplos de clusters formados exclusivamente por esta biblioteca.</p>"
        body += "<table><tr><th>ID Cluster</th><th>Tamaño</th><th>IDs</th><th>Detalle</th></tr>"
        body += "\n".join(rows)
        body += "</table>"

        write_html(page, html_page(f"Duplicados internos — {lib} ({phase})", body))

        index_rows.append(
            f"<tr><td>{esc(lib)}</td><td>{len(clusters)}</td>"
            f"<td><a href='duplicados/{esc(lib)}.html'>Ver ejemplos</a></td></tr>"
        )

    return index_rows


# -------------------------------------------------------------------
# BLOQUE: INTERBIBLIOTECAS
# -------------------------------------------------------------------

def build_interbibliotecas_for_phase(phase: str, data: List[dict], phase_dir: str) -> List[str]:
    """
    Genera los HTML de fusiones interbibliotecas para una fase concreta.
    Devuelve filas HTML para el índice de la fase.
    """
    folder = os.path.join(phase_dir, "interbibliotecas")
    os.makedirs(folder, exist_ok=True)

    inter_by_combo: Dict[str, List[dict]] = {}

    for c in data:
        cid = c.get("cluster_id")
        members = c.get("members", [])
        if not isinstance(members, list):
            continue
        libs = sorted({m.get("lib") for m in members if m.get("lib")})
        if len(libs) <= 1:
            continue
        combo = "+".join(libs)
        inter_by_combo.setdefault(combo, []).append(c)

    index_rows = []

    for combo, clusters in sorted(inter_by_combo.items()):
        page = os.path.join(folder, f"{combo}.html")

        rows = []
        for c in clusters:
            cid = c.get("cluster_id")
            members = c.get("members", [])
            ids_by_lib: Dict[str, List[str]] = {}
            for m in members:
                lib = m.get("lib")
                lid = m.get("local_id")
                if not lib:
                    continue
                ids_by_lib.setdefault(lib, []).append(str(lid))

            parts = []
            for lib, ids in sorted(ids_by_lib.items()):
                parts.append(f"{esc(lib)}: {esc(', '.join(ids))}")
            info = "<br>".join(parts)

            rows.append(
                f"<tr><td>{cid}</td><td>{len(members)}</td>"
                f"<td>{info}</td>"
                f"<td><a href='../sospechosos/cluster_{cid}.html'>Ver cluster</a></td></tr>"
            )

        body = f"<h2>Fusiones interbibliotecas: {esc(combo)} — Fase {esc(phase)}</h2>"
        body += "<p class='small'>Clusters compartidos por varias bibliotecas.</p>"
        body += "<table><tr><th>Cluster</th><th>Tamaño</th><th>IDs por biblioteca</th><th>Detalle</th></tr>"
        body += "\n".join(rows)
        body += "</table>"

        write_html(page, html_page(f"Interbibliotecas — {combo} ({phase})", body))

        index_rows.append(
            f"<tr><td>{esc(combo)}</td><td>{len(clusters)}</td>"
            f"<td><a href='interbibliotecas/{esc(combo)}.html'>Ver ejemplos</a></td></tr>"
        )

    return index_rows


# -------------------------------------------------------------------
# BLOQUE: SOSPECHOSOS (>= 25)
# -------------------------------------------------------------------

def build_sospechosos_for_phase(phase: str, data: List[dict], phase_dir: str) -> int:
    """
    Genera top_50 y un HTML por cluster sospechoso (>= 25 miembros) para la fase.
    Devuelve el nº total de clusters sospechosos.
    """
    folder = os.path.join(phase_dir, "sospechosos")
    os.makedirs(folder, exist_ok=True)

    sospe: List[dict] = []
    for c in data:
        members = c.get("members", [])
        if not isinstance(members, list):
            continue
        if len(members) >= 25:
            sospe.append(c)

    sospe.sort(key=lambda c: len(c.get("members", [])), reverse=True)

    top_50 = sospe[:50]
    rows = []
    for c in top_50:
        cid = c.get("cluster_id")
        members = c.get("members", [])
        libs = sorted({m.get("lib") for m in members if m.get("lib")})
        rows.append(
            f"<tr><td>{cid}</td><td>{len(members)}</td>"
            f"<td>{esc(', '.join(libs))}</td>"
            f"<td><a href='cluster_{cid}.html'>Ver detalle</a></td></tr>"
        )

    body = f"<h2>Top 50 clusters sospechosos — Fase {esc(phase)}</h2>"
    body += "<p class='small'>Clusters con ≥ 25 registros.</p>"
    body += "<table><tr><th>Cluster</th><th>Tamaño</th><th>Bibliotecas</th><th>Detalle</th></tr>"
    body += "\n".join(rows)
    body += "</table>"

    write_html(os.path.join(folder, "top_50.html"),
               html_page(f"Top 50 clusters sospechosos ({phase})", body))

    # HTML por cluster
    for c in sospe:
        cid = c.get("cluster_id")
        members = c.get("members", [])

        ids_by_lib: Dict[str, List[str]] = {}
        for m in members:
            lib = m.get("lib")
            lid = m.get("local_id")
            if not lib:
                continue
            ids_by_lib.setdefault(lib, []).append(str(lid))

        parts = []
        for lib, ids in sorted(ids_by_lib.items()):
            parts.append(f"<h3>{esc(lib)}</h3><p>{esc(', '.join(ids))}</p>")

        body = f"<h2>Cluster {cid} — Fase {esc(phase)}</h2>"
        body += f"<p>Tamaño: {len(members)}</p>"
        body += "".join(parts)

        write_html(os.path.join(folder, f"cluster_{cid}.html"),
                   html_page(f"Cluster {cid} ({phase})", body))

    return len(sospe)


# -------------------------------------------------------------------
# BLOQUE: HEATMAP (TABLA SENCILLA)
# -------------------------------------------------------------------

def build_heatmap_for_phase(phase: str, data: List[dict], phase_dir: str) -> int:
    """
    Genera un heatmap sencillo (tabla) de colisiones interbibliotecas.
    Devuelve el nº de bibliotecas distintas.
    """
    folder = os.path.join(phase_dir, "heatmap")
    os.makedirs(folder, exist_ok=True)

    libs_set = set()
    combos: Dict[str, int] = {}

    for c in data:
        members = c.get("members", [])
        if not isinstance(members, list):
            continue
        libs = sorted({m.get("lib") for m in members if m.get("lib")})
        if len(libs) <= 1:
            continue
        combo = "+".join(libs)
        combos[combo] = combos.get(combo, 0) + 1
        for l in libs:
            libs_set.add(l)

    libs = sorted(libs_set)
    n = len(libs)
    if n == 0:
        body = f"<h2>Mapa de calor interbibliotecas — Fase {esc(phase)}</h2>"
        body += "<p class='small'>No hay clusters interbibliotecas en esta fase.</p>"
        write_html(os.path.join(folder, "heatmap.html"),
                   html_page(f"Mapa de calor ({phase})", body))
        return 0

    idx = {lib: i for i, lib in enumerate(libs)}
    matrix = [[0 for _ in range(n)] for _ in range(n)]

    for combo, count in combos.items():
        parts = combo.split("+")
        for a in parts:
            for b in parts:
                ia = idx[a]
                ib = idx[b]
                matrix[ia][ib] += count

    body = f"<h2>Mapa de calor interbibliotecas — Fase {esc(phase)}</h2>"
    body += "<p class='small'>Matriz de clusters compartidos entre bibliotecas.</p>"

    body += "<table><tr><th></th>"
    for lib in libs:
        body += f"<th>{esc(lib)}</th>"
    body += "</tr>"

    for i, lib in enumerate(libs):
        body += f"<tr><th>{esc(lib)}</th>"
        for j in range(n):
            val = matrix[i][j]
            body += f"<td>{val}</td>"
        body += "</tr>"

    body += "</table>"

    write_html(os.path.join(folder, "heatmap.html"),
               html_page(f"Mapa de calor ({phase})", body))

    return n


# -------------------------------------------------------------------
# INDEX POR FASE
# -------------------------------------------------------------------

def build_phase_index_html(
    phase: str,
    phase_dir: str,
    dup_rows: List[str],
    inter_rows: List[str],
    sospe_count: int,
    num_libs_heatmap: int,
    num_clusters: int,
) -> None:
    rel_phase = esc(phase)

    body = f"<div class='card'><h2>Explorador de ejemplos — Fase {rel_phase}</h2>"
    body += "<p class='small'>Haz clic en los enlaces de cada sección para inspeccionar ejemplos reales.</p>"
    body += "</div>"

    # Info rápida
    body += "<div class='card'>"
    body += "<h2>Resumen rápido de fase</h2>"
    body += "<table>"
    body += "<tr><th>Clusters totales (aprox.)</th><th>Clusters sospechosos (>=25)</th><th>Bibliotecas en heatmap</th></tr>"
    body += f"<tr><td>{num_clusters}</td><td>{sospe_count}</td><td>{num_libs_heatmap}</td></tr>"
    body += "</table>"
    body += "</div>"

    # Duplicados
    body += "<div class='card'><h2>Duplicación interna por biblioteca</h2>"
    if dup_rows:
        body += "<p class='small'>Clusters formados solo por registros de una misma biblioteca (posibles duplicados internos).</p>"
        body += "<table><tr><th>Biblioteca</th><th>Clusters internos</th><th>Enlace</th></tr>"
        body += "\n".join(dup_rows)
        body += "</table>"
    else:
        body += "<p class='small'>No se han detectado clusters internos con más de un registro en esta fase.</p>"
    body += "</div>"

    # Interbibliotecas
    body += "<div class='card'><h2>Fusiones interbibliotecas</h2>"
    if inter_rows:
        body += "<p class='small'>Clusters que agrupan varias bibliotecas (fusión entre catálogos).</p>"
        body += "<table><tr><th>Combinación</th><th>Clusters</th><th>Enlace</th></tr>"
        body += "\n".join(inter_rows)
        body += "</table>"
    else:
        body += "<p class='small'>No se han detectado clusters interbibliotecas en esta fase.</p>"
    body += "</div>"

    # Sospechosos
    body += "<div class='card'><h2>Clusters sospechosos (≥ 25 registros)</h2>"
    if sospe_count:
        body += f"<p class='small'>Total: {sospe_count} clusters.</p>"
        body += "<p><a href='sospechosos/top_50.html'>Ver top 50</a></p>"
    else:
        body += "<p class='small'>No se han detectado clusters grandes (≥ 25) en esta fase.</p>"
    body += "</div>"

    # Heatmap
    body += "<div class='card'><h2>Mapa de calor interbibliotecas</h2>"
    if num_libs_heatmap:
        body += "<p class='small'>Matriz de clusters compartidos entre bibliotecas.</p>"
        body += "<p><a href='heatmap/heatmap.html'>Ver mapa</a></p>"
    else:
        body += "<p class='small'>No hay datos suficientes para construir el mapa de calor en esta fase.</p>"
    body += "</div>"

    # Enlace volver al índice global
    body += """
<div class='card'>
  <p class='small'><a href='../../index.html'>← Volver al índice global de fases</a></p>
</div>
"""

    write_html(os.path.join(phase_dir, "index.html"),
               html_page(f"Explorador de ejemplos — Fase {phase}", body))


# -------------------------------------------------------------------
# INDEX GLOBAL
# -------------------------------------------------------------------

def build_global_index_html(summary_per_phase: List[Dict[str, str]]) -> None:
    body = "<div class='card'><h2>Explorador de ejemplos — Índice global</h2>"
    body += "<p class='small'>Se han detectado las siguientes fases (archivos JSON en tests_output). Haz clic para abrir el explorador de ejemplos de cada una.</p>"
    body += "</div>"

    body += "<div class='card'><h2>Fases disponibles</h2>"
    body += "<table><tr><th>Fase</th><th>Clusters</th><th>Clusters sospechosos (>=25)</th><th>Bibliotecas heatmap</th><th>Enlace</th></tr>"

    for info in summary_per_phase:
        phase = esc(info["phase"])
        num_clusters = info["num_clusters"]
        sospe = info["sospe"]
        libs_heat = info["libs_heat"]
        link = f"fases/{phase}/index.html"
        body += (
            "<tr>"
            f"<td>{phase}</td>"
            f"<td>{num_clusters}</td>"
            f"<td>{sospe}</td>"
            f"<td>{libs_heat}</td>"
            f"<td><a href='{link}'>Abrir explorador</a></td>"
            "</tr>"
        )

    body += "</table></div>"

    write_html(os.path.join(OUT_DIR, "index.html"),
               html_page("Explorador de ejemplos — Índice global", body))


# -------------------------------------------------------------------
# MAIN: GENERAR TODO
# -------------------------------------------------------------------

def cmd_generar():
    print(f"Usando TESTS_DIR = {TESTS_DIR}")
    os.makedirs(OUT_DIR, exist_ok=True)

    phases = detect_phases()
    if not phases:
        print("ERROR: no se han encontrado archivos .json en tests_output.")
        return

    summary_per_phase = []

    for phase in phases:
        print(f"\n=== Procesando fase: {phase} ===")
        data = load_clusters_for_phase(phase)
        if not data:
            print(f"  [!] Fase {phase} sin datos válidos, se omite.")
            continue

        phase_dir = os.path.join(OUT_DIR, "fases", phase)
        os.makedirs(phase_dir, exist_ok=True)

        num_clusters = len(data)

        print("  - Generando duplicados internos...")
        dup_rows = build_duplicados_for_phase(phase, data, phase_dir)

        print("  - Generando interbibliotecas...")
        inter_rows = build_interbibliotecas_for_phase(phase, data, phase_dir)

        print("  - Generando sospechosos...")
        sospe_count = build_sospechosos_for_phase(phase, data, phase_dir)

        print("  - Generando heatmap...")
        num_libs_heatmap = build_heatmap_for_phase(phase, data, phase_dir)

        print("  - Generando índice de fase...")
        build_phase_index_html(
            phase=phase,
            phase_dir=phase_dir,
            dup_rows=dup_rows,
            inter_rows=inter_rows,
            sospe_count=sospe_count,
            num_libs_heatmap=num_libs_heatmap,
            num_clusters=num_clusters,
        )

        summary_per_phase.append({
            "phase": phase,
            "num_clusters": num_clusters,
            "sospe": sospe_count,
            "libs_heat": num_libs_heatmap,
        })

    if not summary_per_phase:
        print("ERROR: ninguna fase tenía datos válidos.")
        return

    print("\n=== Generando índice global ===")
    build_global_index_html(summary_per_phase)

    print("\n✔ Explorador generado en:")
    print(f"  {OUT_DIR}/index.html")


def main():
    parser = argparse.ArgumentParser(
        description="Explorador HTML de ejemplos (auto-finder por fases)"
    )
    sub = parser.add_subparsers(dest="cmd", help="Comando")

    p_gen = sub.add_parser("generar", help="Generar todos los HTML de ejemplos")
    p_gen.set_defaults(func=lambda args: cmd_generar())

    args = parser.parse_args()
    if not hasattr(args, "func"):
        # Si no se pasa comando, por defecto generar
        cmd_generar()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
