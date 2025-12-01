#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ANALIZADOR DE CLUSTERS v2 — HTML ENRIQUECIDO
--------------------------------------------

Mejoras respecto a analizador_clusters.py v1.3:
  ✔ HTML moderno, con resumen general
  ✔ Tabla por bibliotecas del cluster
  ✔ Resumen por campo: nº valores distintos, vacíos, homogéneo/mixto/conflictivo
  ✔ Tabla de valores más frecuentes
  ✔ Diseño responsive sin JS externo (solo CSS)
  ✔ Mantiene compatibilidad total con pipeline v6 mini

Dependencias:
  - PyMARC
  - parse_xml_to_array
"""

import argparse
import json
import csv
import os
import re
from collections import Counter
from pymarc import parse_xml_to_array


# ================================================================
# AUTODETECCIÓN DE LIBX → archivos
# ================================================================

def autodetect_sources():
    """Autodetecta LIB1=xxx.xml desde logs y run_test*."""
    source_map = {}
    patterns = [
        r"--src\s+(LIB\d+)=([^\s]+\.xml)",
        r"--src\s+(LIB\d+)\s*=\s*([^\s]+\.xml)"
    ]
    regexes = [re.compile(p) for p in patterns]

    search_files = []

    for root, dirs, files in os.walk("logs"):
        for f in files:
            if f.endswith(".log"):
                search_files.append(os.path.join(root, f))

    for f in os.listdir("."):
        if f.startswith("run_test") or f.endswith(".log"):
            search_files.append(f)

    for filepath in search_files:
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    for regex in regexes:
                        m = regex.search(line)
                        if m:
                            source_map[m.group(1)] = m.group(2)
        except:
            pass

    return source_map


# ================================================================
# MARC utilities
# ================================================================

def safe_parse_xml(path):
    with open(path, "rb") as fh:
        return parse_xml_to_array(fh)

def index_records_by_001(path):
    recs = safe_parse_xml(path)
    index = {}
    for rec in recs:
        f001 = rec.get_fields("001")
        if f001:
            index[f001[0].data.strip()] = rec
    return index

def extract_fields(record, fields):
    data = {}
    for tag in fields:
        fs = record.get_fields(tag)
        if not fs:
            data[tag] = ""
            continue
        vals = []
        for f in fs:
            try:
                vals.append(" ".join(sf.value for sf in f.subfields))
            except:
                vals.append(getattr(f, "data", ""))
        data[tag] = " || ".join(vals)
    return data


# ================================================================
# ANALIZAR CLUSTER (HTML v2 mejorado)
# ================================================================

def analizar_cluster(cluster_id, phase, fields, source_map, out_prefix):
    json_path = f"tests_output/{phase}.json"
    if not os.path.exists(json_path):
        print(f"❌ No existe {json_path}")
        return

    with open(json_path, "r", encoding="utf-8") as fh:
        clusters = json.load(fh)

    target = next((c for c in clusters if c["cluster_id"] == cluster_id), None)
    if not target:
        print("❌ Cluster no encontrado.")
        return

    members = target["members"]
    total_members = len(members)
    libs = sorted({m["lib"] for m in members})

    # Fichero real
    main_lib = libs[0]
    if main_lib not in source_map:
        print(f"❌ No puedo detectar archivo real para {main_lib}.")
        return

    source_file = source_map[main_lib]
    idx = index_records_by_001(source_file)

    # Recolección
    resumen = {tag: Counter() for tag in fields}
    detalle_registros = []
    valores = []

    # Archivos de salida
    xml_out = f"{out_prefix}.xml"
    csv_out = f"{out_prefix}.csv"
    html_out = f"{out_prefix}.html"

    # -----------------------------------------------------------
    # XML
    # -----------------------------------------------------------
    with open(xml_out, "w", encoding="utf-8") as xf:
        xf.write("<collection>\n")
        for m in members:
            lid = m["local_id"]
            lib = m["lib"]

            if lid not in idx:
                xf.write(f"<!-- Registro {lid} NO encontrado -->\n")
                continue

            rec = idx[lid]
            xf.write(rec.as_xml())

            vals = extract_fields(rec, fields)
            valores.append(vals)

            for tag, val in vals.items():
                resumen[tag][val] += 1

            detalle_registros.append({
                "lib": lib,
                "local_id": lid,
                "title": vals.get("245", "")
            })

        xf.write("</collection>\n")

    # -----------------------------------------------------------
    # CSV
    # -----------------------------------------------------------
    with open(csv_out, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=fields)
        writer.writeheader()
        writer.writerows(valores)

    # -----------------------------------------------------------
    # HTML — ENRIQUECIDO
    # -----------------------------------------------------------
    html = []
    html.append(f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Cluster {cluster_id} — Análisis</title>
<style>
body {{
    font-family: Arial, sans-serif;
    margin: 20px;
}}
h1 {{
    margin-bottom: 5px;
}}
table {{
    border-collapse: collapse;
    width: 100%;
    margin-bottom: 25px;
}}
th, td {{
    border: 1px solid #ccc;
    padding: 6px 8px;
}}
th {{
    background: #eee;
}}
.small {{ font-size: 12px; color: #555; }}
.ok {{ background: #d4ffd4; }}
.mixed {{ background: #fff8c2; }}
.bad {{ background: #ffd4d4; }}
</style>
</head>
<body>
<h1>Cluster {cluster_id} — Fase {phase}</h1>
<p><b>Tamaño:</b> {total_members} registros<br>
<b>Bibliotecas implicadas:</b> {', '.join(libs)}</p>

<h2>Archivos generados</h2>
<ul>
  <li><a href="{os.path.basename(xml_out)}">{xml_out}</a></li>
  <li><a href="{os.path.basename(csv_out)}">{csv_out}</a></li>
</ul>

<h2>Registros del cluster</h2>
<table>
<tr><th>Biblioteca</th><th>ID local</th><th>Título (245$a/b)</th></tr>
""")

    for r in detalle_registros:
        html.append(f"<tr><td>{r['lib']}</td><td>{r['local_id']}</td><td>{r['title']}</td></tr>")

    html.append("</table>")

    # -----------------------------------------------------------
    # POR CAMPO
    # -----------------------------------------------------------
    html.append("<h2>Análisis por campo</h2>")

    for tag in fields:
        counter = resumen[tag]
        total = sum(counter.values())
        distintos = len(counter)
        vacios = counter[""] if "" in counter else 0

        # Determinar estado
        if distintos == 1:
            estado = "ok"
            msg = "Homogéneo"
        elif distintos <= 3:
            estado = "mixed"
            msg = "Variación leve"
        else:
            estado = "bad"
            msg = "Datos conflictivos / múltiples variantes"

        html.append(f"""
        <h3>Campo {tag}</h3>
        <table class="{estado}">
        <tr><th>Total valores</th><th>Distintos</th><th>Vacíos</th><th>Diagnóstico</th></tr>
        <tr><td>{total}</td><td>{distintos}</td><td>{vacios}</td><td>{msg}</td></tr>
        </table>
        """)

        # Valores frecuentes
        html.append("<table><tr><th>Valor</th><th>Frecuencia</th><th>%</th></tr>")
        for val, cnt in counter.most_common(10):
            pct = round((cnt / total_members) * 100, 1)
            display = val if val.strip() else "(vacío)"
            html.append(f"<tr><td>{display}</td><td>{cnt}</td><td>{pct}%</td></tr>")
        html.append("</table>")

    html.append("</body></html>")

    with open(html_out, "w", encoding="utf-8") as hf:
        hf.write("\n".join(html))

    print(f"✔ HTML → {html_out}")
    print(f"✔ XML  → {xml_out}")
    print(f"✔ CSV  → {csv_out}")
    print("✔ Análisis completo.\n")


# ================================================================
# MULTI-CLUSTER
# ================================================================

def analizar_multiples(cluster_ids, phase, fields, source_map):
    for cid in cluster_ids:
        analizar_cluster(cid, phase, fields, source_map, f"cluster_{cid}")


# ================================================================
# MAIN
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="Analizador de clusters — versión 2 (HTML enriquecido)")

    sub = parser.add_subparsers(dest="cmd")

    p1 = sub.add_parser("analizar")
    p1.add_argument("--cluster", type=int, required=True)
    p1.add_argument("--phase", required=True)
    p1.add_argument("--fields", default="245,100,110,111,260,264,300,020,022,035")
    p1.add_argument("--out", default="cluster")

    p2 = sub.add_parser("analizar-multiples")
    p2.add_argument("--clusters", required=True)
    p2.add_argument("--phase", required=True)
    p2.add_argument("--fields", default="245,100,110,111,260,264,300,020,022,035")

    args = parser.parse_args()
    source_map = autodetect_sources()

    if args.cmd == "analizar":
        fields = [x.strip() for x in args.fields.split(",") if x.strip()]
        analizar_cluster(args.cluster, args.phase, fields, source_map, args.out)

    elif args.cmd == "analizar-multiples":
        fields = [x.strip() for x in args.fields.split(",") if x.strip()]
        ids = [int(x) for x in args.clusters.split(",")]
        analizar_multiples(ids, args.phase, fields, source_map)


if __name__ == "__main__":
    main()
