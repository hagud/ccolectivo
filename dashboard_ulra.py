#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dashboard CLI unificado v4 — Catálogo colectivo (colectivo_v6_mini.py)

Características principales:

- Reutiliza la lógica de métricas y análisis de:
    - resumen
    - ranking
    - comparar
    - registro
    - duplicados
    - interbibliotecas
    - peligrosos
    - cluster
    - por-catalogo

- NUEVO sistema de informes HTML (modular y robusto):

  • dashboard_index.html
      - Resumen global de todos los modelos (fases)
      - Ranking de modelos
      - Tabla comparativa
      - Enlaces a un HTML por fase

  • dashboard_<fase>.html (por ejemplo dashboard_01_strong_only.html)
      - Métricas completas de la fase
      - Tabla de duplicación interna por biblioteca
      - Tabla de fusiones interbibliotecas
      - Tabla de clusters sospechosos (tamaño ≥ 25 por defecto)
      - Gráfico Plotly:
            - Clusters por biblioteca (duplicados internos)
            - Clusters por combinación de bibliotecas (interbibliotecas)
            - Mapa de calor de colisiones entre catálogos (si hay interbibliotecas)

- Log de depuración:
    - dashboard_generation.log
      con información sobre:
        * fases detectadas
        * nº de clusters por fase
        * si se ha podido generar cada HTML

Asume que los CSV/JSON están en la carpeta tests_output/ con nombres:
  - 01_strong_only.csv / .json
  - ...
  - 09_final.csv / .json
"""

import os
import csv
import json
import argparse
from typing import Dict, List, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(BASE_DIR, "tests_output")
OUT_INDEX_HTML = os.path.join(BASE_DIR, "dashboard_index.html")
LOG_PATH = os.path.join(BASE_DIR, "dashboard_generation.log")

PHASES = [
    "01_strong_only",
    "02_fuzzy_safe",
    "03_fuzzy_standard",
    "04_fuzzy_aggressive",
    "05_prefer_fields",
    "06_alt_tags",
    "07_fast_nocheck",
    "08_keep9xx",
    "09_final",
]

PHASE_LABELS = {
    "01_strong_only":      "01 — Solo claves fuertes",
    "02_fuzzy_safe":       "02 — Fuzzy seguro (95)",
    "03_fuzzy_standard":   "03 — Fuzzy estándar (92)",
    "04_fuzzy_aggressive": "04 — Fuzzy agresivo (85)",
    "05_prefer_fields":    "05 — Campos preferentes (245/260/264)",
    "06_alt_tags":         "06 — Etiquetas alternativas (991/992/998)",
    "07_fast_nocheck":     "07 — Rápido (sin checksum)",
    "08_keep9xx":          "08 — Conservando 9xx",
    "09_final":            "09 — Modelo final candidato",
}

DEFAULT_PHASE = "09_final"

# Caches
_phase_csv_cache: Dict[str, List[dict]] = {}
_phase_json_cache: Dict[str, List[dict]] = {}
_metrics_cache: Dict[str, dict] = {}
_record_index_cache: Dict[str, Dict[Tuple[str, str], dict]] = {}

# ============================================================
#   LOGGING SENCILLO
# ============================================================

def log(msg: str):
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(msg.rstrip() + "\n")

# ============================================================
#   CARGA DE DATOS (CSV / JSON)
# ============================================================

def load_phase_csv(phase: str) -> List[dict]:
    if phase in _phase_csv_cache:
        return _phase_csv_cache[phase]

    path = os.path.join(TESTS_DIR, f"{phase}.csv")
    rows: List[dict] = []

    if not os.path.exists(path):
        _phase_csv_cache[phase] = []
        log(f"[CSV] No encontrado: {path}")
        return rows

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                size = int(row.get("size", "0") or 0)
            except ValueError:
                size = 0
            row["size"] = size

            libs_raw = row.get("libraries", "") or ""
            row["libraries_list"] = [l for l in libs_raw.split(";") if l.strip()]
            rows.append(row)

    _phase_csv_cache[phase] = rows
    log(f"[CSV] Cargado {path} → {len(rows)} clusters")
    return rows


def load_phase_json(phase: str) -> List[dict]:
    if phase in _phase_json_cache:
        return _phase_json_cache[phase]

    path = os.path.join(TESTS_DIR, f"{phase}.json")
    if not os.path.exists(path):
        _phase_json_cache[phase] = []
        log(f"[JSON] No encontrado: {path}")
        return []

    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)

    _phase_json_cache[phase] = data
    log(f"[JSON] Cargado {path} → {len(data)} clusters")
    return data

# ============================================================
#   MÉTRICAS GLOBALES POR FASE/MODELO
# ============================================================

def compute_metrics(phase: str) -> dict:
    if phase in _metrics_cache:
        return _metrics_cache[phase]

    rows = load_phase_csv(phase)
    n = len(rows)
    if n == 0:
        m = {
            "phase": phase,
            "label": PHASE_LABELS.get(phase, phase),
            "num_clusters": 0,
            "avg_size": 0.0,
            "max_size": 0,
            "perc_size1": 0.0,
            "perc_gt10": 0.0,
            "perc_gt25": 0.0,
            "perc_gt50": 0.0,
            "num_multi_lib": 0,
            "perc_multi_lib": 0.0,
            "num_libs_total": 0,
            "quality_score": 0.0,
            "risk_level": "sin datos",
        }
        _metrics_cache[phase] = m
        log(f"[METRICS] Fase {phase} sin datos")
        return m

    sizes = [r["size"] for r in rows]
    total_size = sum(sizes)
    max_size = max(sizes)
    size1 = sum(1 for s in sizes if s == 1)
    size_gt10 = sum(1 for s in sizes if s > 10)
    size_gt25 = sum(1 for s in sizes if s > 25)
    size_gt50 = sum(1 for s in sizes if s > 50)

    libs_sets = [set(r["libraries_list"]) for r in rows]
    multi_lib = sum(1 for s in libs_sets if len(s) > 1)

    all_libs = set()
    for s in libs_sets:
        all_libs.update(s)

    perc_size1 = size1 / n
    perc_gt10 = size_gt10 / n
    perc_gt25 = size_gt25 / n
    perc_gt50 = size_gt50 / n
    perc_multi_lib = multi_lib / n

    quality_score = (
        0.35 * perc_size1 +
        0.30 * perc_multi_lib -
        0.20 * perc_gt25 -
        0.10 * perc_gt50 -
        0.05 * (max_size / 100.0)
    )

    if max_size > 80 or perc_gt25 > 0.10:
        risk = "alto (probables sobre-fusiones)"
    elif perc_gt25 > 0.03 or perc_gt50 > 0.01:
        risk = "medio (revisar clusters grandes)"
    else:
        risk = "bajo (fusión conservadora)"

    m = {
        "phase": phase,
        "label": PHASE_LABELS.get(phase, phase),
        "num_clusters": n,
        "avg_size": round(total_size / n, 2),
        "max_size": max_size,
        "perc_size1": round(perc_size1, 4),
        "perc_gt10": round(perc_gt10, 4),
        "perc_gt25": round(perc_gt25, 4),
        "perc_gt50": round(perc_gt50, 4),
        "num_multi_lib": multi_lib,
        "perc_multi_lib": round(perc_multi_lib, 4),
        "num_libs_total": len(all_libs),
        "quality_score": round(quality_score, 4),
        "risk_level": risk,
    }

    _metrics_cache[phase] = m
    log(f"[METRICS] Fase {phase}: {n} clusters, max_size={max_size}, score={quality_score:.4f}")
    return m


def get_all_metrics() -> Dict[str, dict]:
    out = {}
    for ph in PHASES:
        m = compute_metrics(ph)
        if m["num_clusters"] > 0:
            out[ph] = m
    return out

# ============================================================
#   ÍNDICE DE REGISTROS (para comando "registro")
# ============================================================

def load_phase_json_for_index(phase: str) -> List[dict]:
    return load_phase_json(phase)

def build_record_index(phase: str) -> Dict[Tuple[str, str], dict]:
    if phase in _record_index_cache:
        return _record_index_cache[phase]

    data = load_phase_json_for_index(phase)
    index: Dict[Tuple[str, str], dict] = {}

    for cluster in data:
        cid = cluster.get("cluster_id")
        members = cluster.get("members", [])
        libs = sorted({m.get("lib") for m in members if m.get("lib")})
        size = len(members)

        for m in members:
            lib = m.get("lib")
            lid = m.get("local_id")
            if not lib or lid is None:
                continue
            key = (str(lib), str(lid))
            index[key] = {
                "phase": phase,
                "cluster_id": cid,
                "size": size,
                "libraries": libs,
            }

    _record_index_cache[phase] = index
    log(f"[INDEX] Construido índice de registros para fase {phase}: {len(index)} entradas")
    return index

# ============================================================
#   ANÁLISIS DE CLUSTERS (duplicados, interbibliotecas, etc.)
# ============================================================

def analyze_internal_duplicates(phase: str):
    data = load_phase_json(phase)
    stats_per_lib: Dict[str, dict] = {}
    clusters_internal: List[dict] = []

    for c in data:
        cid = c.get("cluster_id")
        members = c.get("members", [])
        size = len(members)
        if size <= 1:
            continue

        libs = [m.get("lib") for m in members if m.get("lib")]
        libs_set = set(libs)
        if len(libs_set) == 1:
            lib = list(libs_set)[0]
            st = stats_per_lib.setdefault(lib, {"clusters": 0, "members": 0, "max_size": 0})
            st["clusters"] += 1
            st["members"] += size
            if size > st["max_size"]:
                st["max_size"] = size

            clusters_internal.append({
                "cluster_id": cid,
                "lib": lib,
                "size": size,
                "members": members,
            })

    log(f"[DUP] Fase {phase}: {len(clusters_internal)} clusters internos en {len(stats_per_lib)} bibliotecas")
    return stats_per_lib, clusters_internal


def analyze_interlibrary(phase: str):
    data = load_phase_json(phase)
    stats_per_combo: Dict[str, dict] = {}
    clusters_inter: List[dict] = []

    for c in data:
        cid = c.get("cluster_id")
        members = c.get("members", [])
        size = len(members)
        if size <= 1:
            continue

        libs = sorted({m.get("lib") for m in members if m.get("lib")})
        if len(libs) <= 1:
            continue

        combo = "+".join(libs)
        st = stats_per_combo.setdefault(combo, {"clusters": 0, "members": 0, "max_size": 0})
        st["clusters"] += 1
        st["members"] += size
        if size > st["max_size"]:
            st["max_size"] = size

        clusters_inter.append({
            "cluster_id": cid,
            "libs": libs,
            "size": size,
            "members": members,
        })

    log(f"[INTER] Fase {phase}: {len(clusters_inter)} clusters interbibliotecas en {len(stats_per_combo)} combinaciones")
    return stats_per_combo, clusters_inter


def find_cluster(phase: str, cluster_id: int) -> dict:
    data = load_phase_json(phase)
    for c in data:
        if c.get("cluster_id") == cluster_id:
            return c
    return {}

# ============================================================
#   ANÁLISIS POR CATÁLOGO
# ============================================================

def compute_catalog_stats(phase: str) -> dict:
    data = load_phase_json(phase)
    if not data:
        return {}

    stats = {}

    for c in data:
        members = c.get("members", [])
        libs = [m.get("lib") for m in members]

        for lib in libs:
            if lib not in stats:
                stats[lib] = {
                    "registros_en_colectivo": 0,
                    "clusters_participa": 0,
                    "en_multibib": 0,
                }

            stats[lib]["registros_en_colectivo"] += 1
            stats[lib]["clusters_participa"] += 1

        if len(set(libs)) > 1:
            for lib in libs:
                stats[lib]["en_multibib"] += 1

    total_global = sum(s["registros_en_colectivo"] for s in stats.values()) or 1

    for lib, s in stats.items():
        s["porcentaje"] = round((s["registros_en_colectivo"] / total_global) * 100, 2)
        if s["clusters_participa"] > 0:
            s["participacion_multibib"] = round(
                s["en_multibib"] / s["clusters_participa"] * 100, 2
            )
        else:
            s["participacion_multibib"] = 0.0

    log(f"[CAT] Fase {phase}: {len(stats)} catálogos con participación")
    return stats

# ============================================================
#   DETECCIÓN DE CLUSTERS SOSPECHOSOS (para HTML)
# ============================================================

def get_suspicious_clusters(
    phase: str,
    min_size: int = 25,
    max_clusters: int = 50,
) -> List[dict]:
    data = load_phase_json(phase)
    if not data:
        return []

    sospechosos = []
    for c in data:
        members = c.get("members", [])
        size = len(members)
        if size < min_size:
            continue

        libs = [m.get("lib") for m in members if m.get("lib")]
        libs_set = sorted(set(libs))
        tipo = "interno" if len(libs_set) == 1 else "interbibliotecas"

        by_lib: Dict[str, List[str]] = {}
        for m in members:
            lib = m.get("lib")
            lid = str(m.get("local_id"))
            if not lib:
                continue
            by_lib.setdefault(lib, []).append(lid)

        samples = {}
        for lib, ids in by_lib.items():
            samples[lib] = ids[:5]

        sospechosos.append({
            "cluster_id": c.get("cluster_id"),
            "size": size,
            "libs": libs_set,
            "tipo": tipo,
            "samples": samples,
        })

    sospechosos.sort(key=lambda x: x["size"], reverse=True)
    sospechosos = sospechosos[:max_clusters]
    log(f"[SUSP] Fase {phase}: {len(sospechosos)} clusters sospechosos (min_size={min_size})")
    return sospechosos

# ============================================================
#   COMANDOS CLI EXISTENTES
# ============================================================

def cmd_resumen(args):
    metrics = get_all_metrics()
    if not metrics:
        print("⚠ No hay datos (¿tests_output vacío?).")
        return

    print("\n=== RESUMEN GLOBAL POR MODELO ===\n")

    header = (
        f"{'Modelo':35} "
        f"{'Clusters':>9} "
        f"{'T.Medio':>8} "
        f"{'Max':>6} "
        f"{'%T1':>7} "
        f"{'%>25':>7} "
        f"{'%>50':>7} "
        f"{'%Multi':>7} "
        f"{'Score':>8} "
        f"{'Riesgo':>28}"
    )
    print(header)
    print("-" * len(header))

    for ph in sorted(metrics.keys()):
        m = metrics[ph]
        print(
            f"{m['label'][:33]:35} "
            f"{m['num_clusters']:9d} "
            f"{m['avg_size']:8.2f} "
            f"{m['max_size']:6d} "
            f"{m['perc_size1']*100:6.1f}% "
            f"{m['perc_gt25']*100:6.2f}% "
            f"{m['perc_gt50']*100:6.2f}% "
            f"{m['perc_multi_lib']*100:6.1f}% "
            f"{m['quality_score']:8.4f} "
            f"{m['risk_level'][:28]:28}"
        )

    if args.detallado:
        print("\n--- DETALLE POR MODELO ---\n")
        for ph in sorted(metrics.keys()):
            m = metrics[ph]
            print(f"[{ph}] {m['label']}")
            print(f"  - Clusters totales:         {m['num_clusters']}")
            print(f"  - Tamaño medio de cluster:  {m['avg_size']}")
            print(f"  - Tamaño máximo de cluster: {m['max_size']}")
            print(f"  - % clusters tamaño 1:      {m['perc_size1']*100:.2f}%")
            print(f"  - % clusters > 10:          {m['perc_gt10']*100:.2f}%")
            print(f"  - % clusters > 25:          {m['perc_gt25']*100:.2f}%")
            print(f"  - % clusters > 50:          {m['perc_gt50']*100:.2f}%")
            print(f"  - Clusters multi-biblioteca:{m['num_multi_lib']} "
                  f"({m['perc_multi_lib']*100:.2f}% del total)")
            print(f"  - Bibliotecas distintas:    {m['num_libs_total']}")
            print(f"  - Score de calidad:         {m['quality_score']}")
            print(f"  - Riesgo:                   {m['risk_level']}")
            print()


def cmd_ranking(args):
    metrics = get_all_metrics()
    if not metrics:
        print("⚠ No hay datos.")
        return

    items = []
    for ph, m in metrics.items():
        items.append({
            "phase": ph,
            "label": m["label"],
            "score": m["quality_score"],
            "risk": m["risk_level"],
            "m": m,
        })

    items.sort(key=lambda x: x["score"], reverse=True)

    print("\n=== RANKING DE MODELOS (mejor a peor) ===\n")
    for idx, it in enumerate(items, start=1):
        tag = "  ← RECOMENDADO" if idx == 1 else ""
        print(
            f"{idx:2d}. {it['label'][:40]:40}  "
            f"Score={it['score']:.4f}  Riesgo={it['risk']}{tag}"
        )

    best = items[0]
    print("\n=== CONCLUSIÓN PROPUESTA ===\n")
    print(f"Modelo recomendado: {best['label']}")
    print(f"Score: {best['score']:.4f}")
    print(f"Riesgo: {best['risk']}")
    print("\nHeurística:")
    print("  • Prioriza muchos clusters pequeños (pocos duplicados).")
    print("  • Valora clusters multi-biblioteca (fusión útil entre catálogos).")
    print("  • Penaliza clusters muy grandes (sobre-fusión).")


def cmd_comparar(args):
    metrics = get_all_metrics()
    if not metrics:
        print("⚠ No hay datos.")
        return

    ordered = sorted(metrics.keys())
    print("\n=== COMPARACIÓN ENTRE FASES (consecutivas) ===\n")
    print(f"{'De → A':35} {'ΔClusters':>10} {'ΔT.Medio':>10} {'ΔMax':>8}")
    print("-" * 70)

    for i in range(len(ordered) - 1):
        p1 = ordered[i]
        p2 = ordered[i + 1]
        m1 = metrics[p1]
        m2 = metrics[p2]
        delta_c = m2["num_clusters"] - m1["num_clusters"]
        delta_t = m2["avg_size"] - m1["avg_size"]
        delta_mx = m2["max_size"] - m1["max_size"]
        label = f"{m1['label']} → {m2['label']}"
        print(
            f"{label[:33]:35} "
            f"{delta_c:10d} "
            f"{delta_t:10.2f} "
            f"{delta_mx:8d}"
        )

    if args.detallado:
        print("\n--- ANÁLISIS DETALLADO ---\n")
        for i in range(len(ordered) - 1):
            p1 = ordered[i]
            p2 = ordered[i + 1]
            m1 = metrics[p1]
            m2 = metrics[p2]

            print(f"[{p1} → {p2}] {m1['label']} → {m2['label']}")
            print(f"  - Clusters: {m1['num_clusters']} → {m2['num_clusters']} "
                  f"(Δ {m2['num_clusters'] - m1['num_clusters']})")
            print(f"  - Tamaño medio: {m1['avg_size']} → {m2['avg_size']} "
                  f"(Δ {m2['avg_size'] - m1['avg_size']:.2f})")
            print(f"  - Tamaño máximo: {m1['max_size']} → {m2['max_size']} "
                  f"(Δ {m2['max_size'] - m1['max_size']})")
            print(f"  - % >25: {m1['perc_gt25']*100:.2f}% → "
                  f"{m2['perc_gt25']*100:.2f}%")
            print(f"  - % >50: {m1['perc_gt50']*100:.2f}% → "
                  f"{m2['perc_gt50']*100:.2f}%")
            print(f"  - Score: {m1['quality_score']:.4f} → "
                  f"{m2['quality_score']:.4f}")
            print()


def cmd_registro(args):
    lib = args.lib
    local_id = args.local_id
    print(f"\n=== BÚSQUEDA DE REGISTRO {lib}:{local_id} EN TODAS LAS FASES ===\n")

    found_any = False
    for ph in PHASES:
        idx = build_record_index(ph)
        info = idx.get((lib, local_id))
        label = PHASE_LABELS.get(ph, ph)

        if info:
            found_any = True
            print(f"[{ph}] {label}")
            print(f"  - Cluster ID:      {info['cluster_id']}")
            print(f"  - Tamaño cluster:  {info['size']}")
            print(f"  - Bibliotecas:     {', '.join(info['libraries'])}")
            print()
        else:
            if args.detallado:
                print(f"[{ph}] {label}")
                print("  - El registro no aparece en clusters de esta fase.")
                print()

    if not found_any:
        print("⚠ El registro no aparece en ningún JSON de clusters.")
        print("   Comprueba que las fuentes contienen ese LIB+ID "
              "y que el run_test ha generado los JSON correctamente.")


def cmd_duplicados(args):
    phase = args.phase or DEFAULT_PHASE
    print(f"\n=== DUPLICACIÓN INTERNA POR BIBLIOTECA — Fase {phase} ===\n")

    stats_per_lib, clusters_internal = analyze_internal_duplicates(phase)
    if not clusters_internal:
        print("No se han encontrado clusters internos (mismo catálogo) con más de un registro.")
        return

    print(f"{'Biblioteca':12} {'Clusters':>9} {'Registros':>10} {'MaxSize':>8}")
    print("-" * 45)
    for lib, st in sorted(stats_per_lib.items()):
        print(f"{lib:12} {st['clusters']:9d} {st['members']:10d} {st['max_size']:8d}")

    if args.csv:
        out_dir = TESTS_DIR
        for lib, st in stats_per_lib.items():
            csv_path = os.path.join(out_dir, f"duplicados_internos_{phase}_{lib}.csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["cluster_id", "lib", "cluster_size", "local_id"])
                for c in clusters_internal:
                    if c["lib"] != lib:
                        continue
                    cid = c["cluster_id"]
                    size = c["size"]
                    for m in c["members"]:
                        lid = m.get("local_id")
                        w.writerow([cid, lib, size, lid])
            print(f"✔ CSV duplicados internos {lib}: {csv_path}")


def cmd_interbibliotecas(args):
    phase = args.phase or DEFAULT_PHASE
    print(f"\n=== FUSIONES ENTRE BIBLIOTECAS — Fase {phase} ===\n")

    stats_per_combo, clusters_inter = analyze_interlibrary(phase)
    if not clusters_inter:
        print("No hay clusters con más de una biblioteca.")
        return

    print(f"{'Bibliotecas':25} {'Clusters':>9} {'Registros':>10} {'MaxSize':>8}")
    print("-" * 60)
    for combo, st in sorted(stats_per_combo.items(), key=lambda x: x[0]):
        print(f"{combo:25} {st['clusters']:9d} {st['members']:10d} {st['max_size']:8d}")

    if args.csv:
        csv_path = os.path.join(TESTS_DIR, f"fusiones_interbibliotecas_{phase}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["cluster_id", "cluster_size", "libraries", "lib", "local_id"])
            for c in clusters_inter:
                cid = c["cluster_id"]
                size = c["size"]
                libs = ";".join(c["libs"])
                for m in c["members"]:
                    lib = m.get("lib")
                    lid = m.get("local_id")
                    w.writerow([cid, size, libs, lib, lid])
        print(f"✔ CSV fusiones interbibliotecas: {csv_path}")


def cmd_peligrosos(args):
    phase = args.phase or DEFAULT_PHASE
    min_size = args.min_size
    print(f"\n=== CLUSTERS PELIGROSOS (>= {min_size} registros) — Fase {phase} ===\n")

    data = load_phase_json(phase)
    if not data:
        print("⚠ No hay datos para esta fase.")
        return

    peligrosos = []
    for c in data:
        cid = c.get("cluster_id")
        members = c.get("members", [])
        size = len(members)
        if size < min_size:
            continue
        libs = [m.get("lib") for m in members if m.get("lib")]
        libs_set = set(libs)
        tipo = "interno" if len(libs_set) == 1 else "interbibliotecas"
        peligrosos.append({
            "cluster_id": cid,
            "size": size,
            "libs": sorted(libs_set),
            "tipo": tipo,
        })

    if not peligrosos:
        print("No se han encontrado clusters por encima del umbral.")
        return

    peligrosos.sort(key=lambda x: x["size"], reverse=True)
    print(f"{'Cluster':>8} {'Tam':>6} {'Tipo':>18}  Bibliotecas")
    print("-" * 60)
    for p in peligrosos:
        print(f"{p['cluster_id']:8d} {p['size']:6d} {p['tipo']:>18}  {', '.join(p['libs'])}")

    if args.csv:
        csv_path = os.path.join(TESTS_DIR, f"clusters_peligrosos_{phase}_min{min_size}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["cluster_id", "size", "tipo", "libraries"])
            for p in peligrosos:
                w.writerow([p["cluster_id"], p["size"], p["tipo"], ";".join(p["libs"])])
        print(f"✔ CSV clusters peligrosos: {csv_path}")


def cmd_cluster(args):
    phase = args.phase or DEFAULT_PHASE
    cid = int(args.cluster_id)
    c = find_cluster(phase, cid)
    label = PHASE_LABELS.get(phase, phase)
    print(f"\n=== DETALLE DEL CLUSTER {cid} — Fase {phase} ({label}) ===\n")

    if not c:
        print("⚠ No se ha encontrado ese cluster en el JSON.")
        return

    members = c.get("members", [])
    size = len(members)
    libs = [m.get("lib") for m in members if m.get("lib")]
    libs_set = sorted(set(libs))

    print(f"Tamaño: {size}")
    print(f"Bibliotecas implicadas: {', '.join(libs_set)}")
    print()

    by_lib: Dict[str, List[str]] = {}
    for m in members:
        lib = m.get("lib") or "?"
        lid = str(m.get("local_id"))
        by_lib.setdefault(lib, []).append(lid)

    for lib, ids in sorted(by_lib.items()):
        print(f"[{lib}] {len(ids)} registros:")
        sample = ids if len(ids) <= 30 else ids[:30] + ["..."]
        print("  " + ", ".join(sample))
        print()


def cmd_por_catalogo(args):
    phase = args.phase or DEFAULT_PHASE
    stats = compute_catalog_stats(phase)

    print(f"\n=== ANÁLISIS POR CATÁLOGO — Fase {phase} ===\n")
    print(f"{'LIB':8} {'Registros':>10} {'%':>8} {'MultiBib%':>10}")
    print("-" * 45)
    for lib, s in sorted(stats.items()):
        print(
            f"{lib:8} "
            f"{s['registros_en_colectivo']:10d} "
            f"{s['porcentaje']:8.2f} "
            f"{s['participacion_multibib']:10.2f}"
        )

    if args.csv:
        out_csv = os.path.join(TESTS_DIR, f"por_catalogo_{phase}.csv")
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["lib", "registros", "porcentaje", "multiBib"])
            for lib, s in sorted(stats.items()):
                w.writerow([
                    lib,
                    s["registros_en_colectivo"],
                    s["porcentaje"],
                    s["participacion_multibib"],
                ])
        print(f"✔ CSV generado: {out_csv}")

# ============================================================
#   GENERACIÓN HTML v4 (MODULAR)
# ============================================================

def esc_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )

def generate_index_html(metrics: Dict[str, dict], best_phase: str):
    items = []
    for ph, m in metrics.items():
        items.append({
            "phase": ph,
            "label": m["label"],
            "score": m["quality_score"],
            "risk": m["risk_level"],
            "num_clusters": m["num_clusters"],
        })
    items.sort(key=lambda x: x["score"], reverse=True)

    rows = []
    for it in items:
        link = f"dashboard_{it['phase']}.html"
        label = esc_html(it["label"])
        rows.append(
            f"<tr>"
            f"<td><a href=\"{link}\">{label}</a></td>"
            f"<td>{it['num_clusters']}</td>"
            f"<td>{it['score']:.4f}</td>"
            f"<td>{esc_html(it['risk'])}</td>"
            f"</tr>"
        )

    best = items[0]
    best_label = esc_html(best["label"])
    best_risk = esc_html(best["risk"])

    html = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Dashboard catálogo colectivo — Índice</title>
<style>
body {
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    background: #f5f7fb;
    margin: 0;
}
header {
    background: #ffffff;
    border-bottom: 1px solid #d0d7e2;
    padding: 16px 24px;
}
header h1 {
    margin: 0;
    font-size: 20px;
    font-weight: 600;
}
header small {
    font-size: 12px;
    color: #6b7280;
}
main {
    padding: 20px 24px 40px;
}
.card {
    background: #ffffff;
    border-radius: 10px;
    border: 1px solid #d0d7e2;
    padding: 16px 18px;
    margin-bottom: 18px;
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin-top: 8px;
}
th, td {
    border: 1px solid #d0d7e2;
    padding: 6px 4px;
    text-align: center;
}
th {
    background: #eef2f7;
}
.small {
    font-size: 12px;
    color: #6b7280;
}
</style>
</head>
<body>

<header>
  <h1>Dashboard catálogo colectivo — Índice</h1>
  <small>Informe generado automáticamente desde tests_output (colectivo_v6_mini.py)</small>
</header>

<main>

<div class="card">
  <h2>Modelo recomendado</h2>
  <p><b>""" + best_label + """</b></p>
  <p>Score: """ + f"{best['score']:.4f}" + """</p>
  <p>Riesgo: """ + best_risk + """</p>
  <p class="small">
    Haz clic en el modelo para ver el análisis completo por fase.
  </p>
</div>

<div class="card">
  <h2>Resumen de modelos</h2>
  <table>
    <tr>
      <th>Modelo</th>
      <th>Clusters</th>
      <th>Score</th>
      <th>Riesgo</th>
    </tr>
"""

    html += "\n".join(rows)

    html += """
  </table>
</div>

</main>
</body>
</html>
"""

    with open(OUT_INDEX_HTML, "w", encoding="utf-8") as fh:
        fh.write(html)
    log(f"[HTML] Generado índice: {OUT_INDEX_HTML}")


def generate_phase_html(phase: str, metrics: dict):
    label = metrics["label"]
    safe_label = esc_html(label)
    phase_file = os.path.join(BASE_DIR, f"dashboard_{phase}.html")

    # Datos de la fase
    dup_stats, _ = analyze_internal_duplicates(phase)
    inter_stats, _ = analyze_interlibrary(phase)
    suspicious = get_suspicious_clusters(phase, min_size=25, max_clusters=50)

    libs_dup = sorted(dup_stats.keys())
    dup_clusters_per_lib = [dup_stats[lib]["clusters"] for lib in libs_dup]

    combos_inter = sorted(inter_stats.keys())
    inter_clusters_per_combo = [inter_stats[c]["clusters"] for c in combos_inter]

    # Matriz heatmap interbibliotecas
    libs_set = set()
    for combo in combos_inter:
        parts = combo.split("+")
        for p in parts:
            libs_set.add(p)
    libs_heat = sorted(libs_set)
    n = len(libs_heat)
    matrix = [[0 for _ in range(n)] for _ in range(n)]
    idx = {lib: i for i, lib in enumerate(libs_heat)}

    for combo, st in inter_stats.items():
        cs = st["clusters"]
        parts = combo.split("+")
        if len(parts) < 2:
            continue
        for a in parts:
            for b in parts:
                ia = idx[a]
                ib = idx[b]
                matrix[ia][ib] += cs

    # Tabla HTML de duplicados
    if not libs_dup:
        dup_table_html = "<p class=\"small\">No se han detectado clusters internos con más de un registro en esta fase.</p>"
    else:
        rows = ["<table><tr><th>Biblioteca</th><th>Clusters internos</th><th>Registros implicados</th><th>Máx tamaño cluster</th></tr>"]
        for lib in libs_dup:
            st = dup_stats[lib]
            rows.append(
                "<tr>"
                f"<td>{esc_html(lib)}</td>"
                f"<td>{st['clusters']}</td>"
                f"<td>{st['members']}</td>"
                f"<td>{st['max_size']}</td>"
                "</tr>"
            )
        rows.append("</table>")
        dup_table_html = "\n".join(rows)

    # Tabla HTML de interbibliotecas
    if not combos_inter:
        inter_table_html = "<p class=\"small\">No se han detectado clusters con más de una biblioteca en esta fase.</p>"
    else:
        rows = ["<table><tr><th>Bibliotecas</th><th>Clusters compartidos</th><th>Registros implicados</th><th>Máx tamaño cluster</th></tr>"]
        for combo in combos_inter:
            st = inter_stats[combo]
            rows.append(
                "<tr>"
                f"<td>{esc_html(combo)}</td>"
                f"<td>{st['clusters']}</td>"
                f"<td>{st['members']}</td>"
                f"<td>{st['max_size']}</td>"
                "</tr>"
            )
        rows.append("</table>")
        inter_table_html = "\n".join(rows)

    # Tabla HTML de clusters sospechosos
    if not suspicious:
        suspicious_table_html = "<p class=\"small\">No se han detectado clusters grandes (≥ 25 registros) en esta fase.</p>"
    else:
        rows = [
            "<table><tr>"
            "<th>Cluster</th>"
            "<th>Tamaño</th>"
            "<th>Tipo</th>"
            "<th>Bibliotecas</th>"
            "<th>Muestras de IDs por biblioteca</th>"
            "</tr>"
        ]
        for s in suspicious:
            libs = ", ".join(s["libs"])
            muestras_parts = []
            for lib, ids in sorted(s["samples"].items()):
                muestras_parts.append(f"{lib}: " + ", ".join(ids))
            muestras_str = "<br>".join(esc_html(x) for x in muestras_parts)
            rows.append(
                "<tr>"
                f"<td>{s['cluster_id']}</td>"
                f"<td>{s['size']}</td>"
                f"<td>{esc_html(s['tipo'])}</td>"
                f"<td>{esc_html(libs)}</td>"
                f"<td>{muestras_str}</td>"
                "</tr>"
            )
        rows.append("</table>")
        suspicious_table_html = "\n".join(rows)

    # Construcción HTML de la fase
    html = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Dashboard catálogo colectivo — Fase """ + esc_html(phase) + """</title>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<style>
body {
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    background: #f5f7fb;
    margin: 0;
}
header {
    background: #ffffff;
    border-bottom: 1px solid #d0d7e2;
    padding: 16px 24px;
}
header h1 {
    margin: 0;
    font-size: 20px;
    font-weight: 600;
}
header small {
    font-size: 12px;
    color: #6b7280;
}
main {
    padding: 20px 24px 40px;
}
.card {
    background: #ffffff;
    border-radius: 10px;
    border: 1px solid #d0d7e2;
    padding: 16px 18px;
    margin-bottom: 18px;
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin-top: 8px;
}
th, td {
    border: 1px solid #d0d7e2;
    padding: 6px 4px;
    text-align: center;
}
th {
    background: #eef2f7;
}
h2 {
    margin: 0 0 6px 0;
}
.small {
    font-size: 12px;
    color: #6b7280;
}
nav a {
    text-decoration: none;
    color: #2563eb;
    font-size: 13px;
}
nav {
    margin-bottom: 10px;
}
</style>
</head>
<body>

<header>
  <h1>Dashboard catálogo colectivo — Fase """ + esc_html(phase) + """</h1>
  <small>""" + safe_label + """</small>
</header>

<main>

<nav>
  <a href="dashboard_index.html">← Volver al índice</a>
</nav>

<div class="card">
  <h2>Métricas de la fase</h2>
  <table>
    <tr><th>Clusters</th><th>Tamaño medio</th><th>Tamaño máx.</th><th>% tamaño 1</th><th>% multi-bib</th><th>Score</th><th>Riesgo</th></tr>
    <tr>
      <td>""" + str(metrics["num_clusters"]) + """</td>
      <td>""" + f"{metrics['avg_size']:.2f}" + """</td>
      <td>""" + str(metrics["max_size"]) + """</td>
      <td>""" + f"{metrics['perc_size1']*100:.2f}%" + """</td>
      <td>""" + f"{metrics['perc_multi_lib']*100:.2f}%" + """</td>
      <td>""" + f"{metrics['quality_score']:.4f}" + """</td>
      <td>""" + esc_html(metrics["risk_level"]) + """</td>
    </tr>
  </table>
</div>

<div class="card">
  <h2>Duplicación interna por biblioteca</h2>
  <p class="small">
    Clusters formados exclusivamente por registros de una misma biblioteca. Indican duplicados internos o problemas de migración.
  </p>
  """ + dup_table_html + """
  <div id="chartDup" style="height:320px;margin-top:12px;"></div>
</div>

<div class="card">
  <h2>Fusiones entre bibliotecas</h2>
  <p class="small">
    Clusters que agrupan registros de varias bibliotecas. Valores razonables indican coincidencias reales; valores excesivos pueden señalar sobre-fusión.
  </p>
  """ + inter_table_html + """
  <div id="chartInter" style="height:320px;margin-top:12px;"></div>
</div>

<div class="card">
  <h2>Mapa de calor de colisiones entre catálogos</h2>
  <p class="small">
    Muestra el número de clusters compartidos entre pares de bibliotecas en esta fase.
  </p>
  <div id="heatmapInter" style="height:420px;"></div>
</div>

<div class="card">
  <h2>Clusters sospechosos (tamaño ≥ 25)</h2>
  <p class="small">
    Clusters de gran tamaño que conviene revisar manualmente. Revisa especialmente los interbibliotecas con muchas combinaciones y variación de IDs.
  </p>
  """ + suspicious_table_html + """
</div>

</main>

<script>
"""

    # Datos JS para gráficos
    html += "const libsDup = " + json.dumps(libs_dup) + ";\n"
    html += "const dupClusters = " + json.dumps(dup_clusters_per_lib) + ";\n"
    html += "const combosInter = " + json.dumps(combos_inter) + ";\n"
    html += "const interClusters = " + json.dumps(inter_clusters_per_combo) + ";\n"
    html += "const heatLibs = " + json.dumps(libs_heat) + ";\n"
    html += "const heatMatrix = " + json.dumps(matrix) + ";\n"

    # Funciones JS de gráficos
    html += """
// Gráfico duplicados internos por biblioteca
(function(){
  if (!libsDup.length) {
    document.getElementById('chartDup').innerHTML = '<p class="small">Sin datos de duplicación interna en esta fase.</p>';
    return;
  }
  Plotly.newPlot('chartDup', [{
    x: libsDup,
    y: dupClusters,
    type: 'bar',
    name: 'Clusters internos'
  }], {
    title: 'Clusters internos por biblioteca',
    margin: {t: 40, b: 80},
    xaxis: {tickangle: -35}
  });
})();

// Gráfico fusiones interbibliotecas
(function(){
  if (!combosInter.length) {
    document.getElementById('chartInter').innerHTML = '<p class="small">Sin datos interbibliotecas en esta fase.</p>';
    return;
  }
  Plotly.newPlot('chartInter', [{
    x: combosInter,
    y: interClusters,
    type: 'bar',
    name: 'Clusters interbibliotecas'
  }], {
    title: 'Clusters interbibliotecas por combinación de catálogos',
    margin: {t: 40, b: 110},
    xaxis: {tickangle: -60}
  });
})();

// Heatmap interbibliotecas
(function(){
  if (!heatLibs.length) {
    document.getElementById('heatmapInter').innerHTML = '<p class="small">No hay datos suficientes para construir el mapa de calor.</p>';
    return;
  }
  const data = [{
    z: heatMatrix,
    x: heatLibs,
    y: heatLibs,
    type: 'heatmap',
    hoverongaps: false
  }];
  const layout = {
    title: 'Mapa de calor interbibliotecas — fase """ + esc_html(phase) + """',
    margin: {t: 40, b: 100, l: 100},
    xaxis: {tickangle: -35}
  };
  Plotly.newPlot('heatmapInter', data, layout);
})();
</script>

</body>
</html>
"""

    with open(phase_file, "w", encoding="utf-8") as fh:
        fh.write(html)
    log(f"[HTML] Generado HTML de fase: {phase_file}")


def cmd_generar_html(args):
    # Limpia log previo
    if os.path.exists(LOG_PATH):
        os.remove(LOG_PATH)
    log("=== Generación de dashboards HTML (v4) ===")

    metrics = get_all_metrics()
    if not metrics:
        print("⚠ No hay datos, no se puede generar HTML.")
        log("[ERROR] Sin metrics: no se generan HTMLs")
        return

    # Fase recomendada = mejor score
    best_phase = sorted(metrics.keys(), key=lambda p: metrics[p]["quality_score"], reverse=True)[0]
    log(f"[BEST] Fase recomendada: {best_phase} ({metrics[best_phase]['label']})")

    # HTML índice
    generate_index_html(metrics, best_phase)

    # HTML por fase
    for ph in sorted(metrics.keys()):
        if metrics[ph]["num_clusters"] == 0:
            continue
        generate_phase_html(ph, metrics[ph])

    print("✔ HTMLs generados:")
    print(f"   - Índice: {OUT_INDEX_HTML}")
    for ph in sorted(metrics.keys()):
        if metrics[ph]["num_clusters"] == 0:
            continue
        print(f"   - Fase {ph}: dashboard_{ph}.html")

    print(f"\nLog de generación: {LOG_PATH}")

# ============================================================
#   MAIN / ARGPARSE
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Dashboard CLI unificado catálogo colectivo (tests_output)"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_res = sub.add_parser("resumen", help="mostrar resumen por modelo")
    p_res.add_argument("--detallado", action="store_true",
                       help="muestra detalle por modelo")
    p_res.set_defaults(func=cmd_resumen)

    p_rank = sub.add_parser("ranking", help="mostrar ranking de modelos")
    p_rank.set_defaults(func=cmd_ranking)

    p_comp = sub.add_parser("comparar",
                            help="comparar modelos consecutivos")
    p_comp.add_argument("--detallado", action="store_true",
                        help="muestra análisis ampliado")
    p_comp.set_defaults(func=cmd_comparar)

    p_reg = sub.add_parser("registro",
                           help="ver evolución de un registro en las fases")
    p_reg.add_argument("lib", help="Código de biblioteca (ej. LIB1)")
    p_reg.add_argument("local_id", help="ID local (999$c / 001)")
    p_reg.add_argument("--detallado", action="store_true",
                       help="muestra también fases donde no aparece")
    p_reg.set_defaults(func=cmd_registro)

    p_dup = sub.add_parser("duplicados",
                           help="analizar duplicación interna por biblioteca")
    p_dup.add_argument("--phase", help=f"fase a analizar (por defecto {DEFAULT_PHASE})")
    p_dup.add_argument("--csv", action="store_true",
                       help="exportar CSV con los duplicados internos")
    p_dup.set_defaults(func=cmd_duplicados)

    p_inter = sub.add_parser("interbibliotecas",
                             help="analizar fusiones entre bibliotecas")
    p_inter.add_argument("--phase", help=f"fase a analizar (por defecto {DEFAULT_PHASE})")
    p_inter.add_argument("--csv", action="store_true",
                         help="exportar CSV con las fusiones entre bibliotecas")
    p_inter.set_defaults(func=cmd_interbibliotecas)

    p_pel = sub.add_parser("peligrosos",
                           help="listar clusters muy grandes (posible sobre-fusión)")
    p_pel.add_argument("--phase", help=f"fase a analizar (por defecto {DEFAULT_PHASE})")
    p_pel.add_argument("--min-size", type=int, default=50,
                       help="tamaño mínimo del cluster para considerarlo peligroso (por defecto 50)")
    p_pel.add_argument("--csv", action="store_true",
                       help="exportar CSV con los clusters peligrosos")
    p_pel.set_defaults(func=cmd_peligrosos)

    p_cl = sub.add_parser("cluster",
                          help="inspeccionar un cluster concreto")
    p_cl.add_argument("--phase", help=f"fase a analizar (por defecto {DEFAULT_PHASE})")
    p_cl.add_argument("cluster_id", help="ID de cluster (número entero)")
    p_cl.set_defaults(func=cmd_cluster)

    p_cat = sub.add_parser("por-catalogo",
                           help="análisis por catálogo (LIB) en una fase")
    p_cat.add_argument("--phase", default=DEFAULT_PHASE,
                       help=f"fase a analizar (por defecto {DEFAULT_PHASE})")
    p_cat.add_argument("--csv", action="store_true",
                       help="exportar CSV con resumen por catálogo")
    p_cat.set_defaults(func=cmd_por_catalogo)

    p_html = sub.add_parser("generar_html",
                            help="generar dashboard_index.html + dashboard_<fase>.html")
    p_html.set_defaults(func=cmd_generar_html)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
