#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
colectivo_v6_monitor.py

Versión de diagnóstico de tu script de catálogo colectivo (v6_mini).

OBJETIVO:
- Mantener la MISMA interfaz de línea de comandos.
- NO cambiar la lógica ni los resultados.
- Añadir instrumentación avanzada:
  * Tiempo por fase
  * Uso de RAM por fase y picos
  * Tamaño de estructuras internas
  * Estadísticas del fuzzy
  * Histograma de tamaños de clusters
  * Trazas de memoria con tracemalloc
  * Resumen humano legible

NOTA:
- Debes copiar/pegar la lógica interna de tu `colectivo_v6_mini.py` dentro de
  los bloques marcados con:
     # >>> AQUÍ VA LA LÓGICA DE ...
"""

import argparse
import os
import sys
import time
import logging
import gc
import tracemalloc
from contextlib import contextmanager

try:
    import psutil
except ImportError:
    psutil = None  # El script sigue funcionando, pero con menos info de memoria

# =====================================================================
# CONFIGURACIÓN DE LOGS
# =====================================================================

LOG_DIR = "logs"

LOG_FILES = {
    "performance": "performance.log",
    "memory_peaks": "memory_peaks.log",
    "structures": "structures_sizes.log",
    "fuzzy": "fuzzy_stats.log",
    "clusters": "cluster_histogram.log",
    "memory_sources": "memory_sources.log",
    "gc": "gc_effect.log",
    "summary": "summary_readable.log",
}

LOGGERS = {}


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)

    fmt_technical = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fmt_human = logging.Formatter(
        fmt="%(message)s"
    )

    for name, filename in LOG_FILES.items():
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Evitar duplicar handlers si se llama más de una vez
        if logger.handlers:
            continue

        handler = logging.FileHandler(os.path.join(LOG_DIR, filename), mode="w", encoding="utf-8")
        if name == "summary":
            handler.setFormatter(fmt_human)
        else:
            handler.setFormatter(fmt_technical)

        logger.addHandler(handler)
        LOGGERS[name] = logger


def get_logger(name):
    return LOGGERS.get(name)


# =====================================================================
# MONITOR DE RENDIMIENTO
# =====================================================================

class PerfMonitor:
    def __init__(self, enable_tracemalloc=True, peak_interval=5000):
        self.process = psutil.Process(os.getpid()) if psutil else None
        self.enable_tracemalloc = enable_tracemalloc
        self.peak_interval = peak_interval

        self.last_mem = self.get_mem_mb()
        self.phase_counter = 0

        if self.enable_tracemalloc:
            tracemalloc.start()

        self.log_perf = get_logger("performance")
        self.log_peaks = get_logger("memory_peaks")
        self.log_struct = get_logger("structures")
        self.log_fuzzy = get_logger("fuzzy")
        self.log_clusters = get_logger("clusters")
        self.log_memsrc = get_logger("memory_sources")
        self.log_gc = get_logger("gc")
        self.log_summary = get_logger("summary")

        self.log_summary.write = self.log_summary.info  # alias mental

        self.log_summary.info("========================================")
        self.log_summary.info("      RESUMEN DE EJECUCIÓN COLECTIVO    ")
        self.log_summary.info("========================================\n")

    # ------------------ Utilidades básicas ------------------

    def get_mem_mb(self):
        if not self.process:
            return None
        return self.process.memory_info().rss / (1024 * 1024)

    def log_peak(self, phase_name, step_desc=None):
        mem = self.get_mem_mb()
        if mem is None:
            return
        msg = f"[PEAK] fase={phase_name} step={step_desc or '-'} mem={mem:.2f}MB"
        self.log_peaks.info(msg)

    def log_struct_size(self, name, obj, extra=""):
        """Log simple del tamaño de una estructura (len)."""
        try:
            size = len(obj)
        except Exception:
            size = "NA"
        msg = f"[STRUCT] {name} size={size} {extra}"
        self.log_struct.info(msg)

    def log_tracemalloc_top(self, phase_name, limit=20):
        if not self.enable_tracemalloc:
            return
        current, peak = tracemalloc.get_traced_memory()
        self.log_memsrc.info(
            "[TRACEMALLOC] fase=%s current=%.2fMB peak=%.2fMB",
            phase_name,
            current / (1024 * 1024),
            peak / (1024 * 1024),
        )

        stats = tracemalloc.take_snapshot().statistics("lineno")
        self.log_memsrc.info("[TRACEMALLOC] Top %d líneas más consumidoras:", limit)
        for i, stat in enumerate(stats[:limit], start=1):
            self.log_memsrc.info("  #%d %s", i, stat)

    def gc_collect_with_log(self, phase_name):
        before = self.get_mem_mb()
        collected = gc.collect()
        after = self.get_mem_mb()
        if before is not None and after is not None:
            self.log_gc.info(
                "[GC] fase=%s collected=%d mem_before=%.2fMB mem_after=%.2fMB delta=%.2fMB",
                phase_name,
                collected,
                before,
                after,
                after - before,
            )
        else:
            self.log_gc.info("[GC] fase=%s collected=%d", phase_name, collected)

    # ------------------ Context manager por fase ------------------

    @contextmanager
    def phase(self, name, description=None):
        """
        Uso:
            with monitor.phase("lectura_mrc", "Lectura de ficheros MRC/XML"):
                # ... tu lógica ...
        """
        self.phase_counter += 1
        phase_id = self.phase_counter

        t0 = time.perf_counter()
        mem_before = self.get_mem_mb()

        self.log_perf.info("[PHASE_START] id=%d name=%s mem=%.2fMB", phase_id, name, mem_before or -1)
        if description:
            self.log_summary.info(f"--- FASE {phase_id}: {name} ---")
            self.log_summary.info(f"Descripción: {description}")

        try:
            yield
        finally:
            t1 = time.perf_counter()
            mem_after = self.get_mem_mb()
            delta_t = t1 - t0
            delta_mem = (mem_after - mem_before) if (mem_after is not None and mem_before is not None) else None

            self.log_perf.info(
                "[PHASE_END] id=%d name=%s time=%.3fs mem_before=%.2fMB mem_after=%.2fMB delta_mem=%.2fMB",
                phase_id,
                name,
                delta_t,
                mem_before or -1,
                mem_after or -1,
                delta_mem or 0.0,
            )

            self.log_summary.info(f"Tiempo: {delta_t:.2f} s")
            if delta_mem is not None:
                self.log_summary.info(f"Memoria: {mem_before:.2f} MB → {mem_after:.2f} MB (Δ {delta_mem:.2f} MB)")
            self.log_summary.info("")

    # ------------------ Logs específicos para fuzzy ------------------

    def log_fuzzy_stats(
        self,
        total_candidates,
        total_comparisons,
        skipped_by_prefilter,
        total_matches,
        avg_similarity,
        bucket_sizes,
    ):
        """
        bucket_sizes: lista de tamaños de buckets (para histograma simple)
        """
        self.log_fuzzy.info(
            "[FUZZY] candidates=%d comparisons=%d skipped_prefilter=%d matches=%d avg_sim=%.2f",
            total_candidates,
            total_comparisons,
            skipped_by_prefilter,
            total_matches,
            avg_similarity,
        )

        if bucket_sizes:
            max_bucket = max(bucket_sizes)
            avg_bucket = sum(bucket_sizes) / len(bucket_sizes)
            self.log_fuzzy.info(
                "[FUZZY_BUCKETS] count=%d avg=%.2f max=%d", len(bucket_sizes), avg_bucket, max_bucket
            )

        # Resumen legible
        self.log_summary.info(">>> Resumen Fuzzy")
        self.log_summary.info(f"  Candidatos totales: {total_candidates}")
        self.log_summary.info(f"  Comparaciones reales: {total_comparisons}")
        self.log_summary.info(f"  Saltados por pre-filtro: {skipped_by_prefilter}")
        self.log_summary.info(f"  Matches totales: {total_matches}")
        self.log_summary.info(f"  Similitud media: {avg_similarity:.2f}")
        if bucket_sizes:
            self.log_summary.info(f"  Tamaño medio de bucket: {avg_bucket:.2f}")
            self.log_summary.info(f"  Tamaño máximo de bucket: {max_bucket}")
        self.log_summary.info("")

    # ------------------ Histograma de tamaños de clusters ------------------

    def log_cluster_histogram(self, cluster_sizes_dict):
        """
        cluster_sizes_dict: {tamaño_cluster: cantidad_de_clusters}
        """
        self.log_clusters.info("[CLUSTERS_HISTOGRAM_START]")
        for size, count in sorted(cluster_sizes_dict.items()):
            self.log_clusters.info("  size=%d count=%d", size, count)
        self.log_clusters.info("[CLUSTERS_HISTOGRAM_END]")

        # Resumen legible
        total_clusters = sum(cluster_sizes_dict.values())
        self.log_summary.info(">>> Resumen Clusters")
        self.log_summary.info(f"  Nº total de clusters: {total_clusters}")
        for size, count in sorted(cluster_sizes_dict.items()):
            self.log_summary.info(f"  Tamaño {size}: {count} clusters")
        self.log_summary.info("")


# =====================================================================
# PARSER DE ARGUMENTOS (aprox. interfaz v6_mini)
# =====================================================================

def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Catálogo colectivo v6 (monitor de rendimiento)."
    )

    parser.add_argument(
        "--src",
        action="append",
        metavar="LIB=FICHERO",
        help="Fuente de datos. Ejemplo: --src LIB1=agn.mrc (se puede repetir)",
        required=True,
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Fichero de salida MARCXML fusionado.",
    )
    parser.add_argument(
        "--report",
        required=True,
        help="CSV de clusters con resumen.",
    )
    parser.add_argument(
        "--clusters-json",
        dest="clusters_json",
        required=True,
        help="JSON con detalle de clusters.",
    )

    parser.add_argument(
        "--no-fuzzy",
        action="store_true",
        help="Desactiva el matching difuso (solo strong).",
    )
    parser.add_argument(
        "--strong-only",
        action="store_true",
        help="Solo clustering fuerte (sin segunda pasada).",
    )
    parser.add_argument(
        "--weak-threshold",
        type=int,
        default=97,
        help="Umbral de similitud para fuzzy (por defecto 97).",
    )

    # Opcional: nivel de monitorización
    parser.add_argument(
        "--monitor-minimal",
        action="store_true",
        help="Modo monitor mínimo (sin tracemalloc, menos logs).",
    )

    return parser


# =====================================================================
# EJECUCIÓN PRINCIPAL CON MONITOR
# =====================================================================

def run_collective_with_monitor(args):
    """
    Esta función debe replicar la lógica de tu colectivo_v6_mini, pero rodeada de
    bloques de monitorización.

    IMPORTANTE:
    - Copia aquí la estructura principal de tu v6_mini (lectura, strong, fuzzy, fusión, escritura).
    - NO cambies la lógica, solo encapsula en `with monitor.phase(...)`.
    """

    setup_logging()
    monitor = PerfMonitor(enable_tracemalloc=not args.monitor_minimal)

    log_summary = get_logger("summary")
    log_summary.info("Fuentes de entrada:")
    for src in args.src:
        log_summary.info(f"  - {src}")
    log_summary.info(f"Salida MARCXML: {args.out}")
    log_summary.info(f"Reporte CSV:    {args.report}")
    log_summary.info(f"Clusters JSON:  {args.clusters_json}")
    log_summary.info("")

    # -----------------------------------------------------------------
    # EJEMPLO DE ESTRUCTURA DE FASES
    # Ajusta los nombres de fases y pega tu lógica en cada bloque.
    # -----------------------------------------------------------------

    # 1) Lectura e indexación de registros
    with monitor.phase("lectura_indexacion", "Lectura de fuentes MRC/XML e indexación inicial"):
        # >>> AQUÍ VA LA LÓGICA DE LECTURA DE FICHEROS DE TU v6_mini
        # Ejemplo conceptual:
        #   records_por_biblioteca = leer_fuentes(args.src)
        #   index_global = construir_indice(records_por_biblioteca)
        #
        records_por_biblioteca = {}  # placeholder
        index_global = {}            # placeholder

        # Puedes registrar tamaño de estructuras:
        monitor.log_struct_size("records_por_biblioteca", records_por_biblioteca)
        monitor.log_struct_size("index_global", index_global)

        monitor.log_peak("lectura_indexacion", "fin_fase")

    monitor.gc_collect_with_log("post_lectura")

    # 2) Clustering fuerte (strong)
    with monitor.phase("clustering_strong", "Clustering fuerte (strong-only)"):
        # >>> AQUÍ VA LA LÓGICA DE CLUSTERING FUERTE DE TU v6_mini
        #
        #   clusters_strong = hacer_clustering_strong(index_global)
        #
        clusters_strong = {}  # placeholder

        monitor.log_struct_size("clusters_strong", clusters_strong)
        monitor.log_peak("clustering_strong", "fin_fase")

    monitor.gc_collect_with_log("post_strong")

    # 3) Clustering difuso (fuzzy) – si procede
    clusters_final = None
    if not args.no_fuzzy and not args.strong_only:
        with monitor.phase("clustering_fuzzy", "Segunda pasada con fuzzy matching"):
            # >>> AQUÍ VA LA LÓGICA DE FUZZY DE TU v6_mini
            #
            # Ejemplo conceptual de variables que puedes calcular
            total_candidates = 0
            total_comparisons = 0
            skipped_by_prefilter = 0
            total_matches = 0
            avg_similarity = 0.0
            bucket_sizes = []

            # clusters_final = aplicar_fuzzy(clusters_strong, umbral=args.weak_threshold)
            clusters_final = clusters_strong  # placeholder

            # Una vez tengas las métricas reales, rellena las variables de arriba
            monitor.log_struct_size("clusters_final", clusters_final)
            monitor.log_peak("clustering_fuzzy", "fin_fase")

            # Log de estadísticas de fuzzy (cuando tengas datos reales)
            monitor.log_fuzzy_stats(
                total_candidates=total_candidates,
                total_comparisons=total_comparisons,
                skipped_by_prefilter=skipped_by_prefilter,
                total_matches=total_matches,
                avg_similarity=avg_similarity,
                bucket_sizes=bucket_sizes,
            )

        monitor.gc_collect_with_log("post_fuzzy")
    else:
        clusters_final = clusters_strong

    # 4) Histograma de tamaños de clusters
    with monitor.phase("analisis_clusters", "Cálculo de histograma de tamaños de clusters"):
        # >>> Construye un diccionario {tamaño: cantidad}
        #
        # Ejemplo:
        #   hist = {}
        #   for cluster_id, miembros in clusters_final.items():
        #       size = len(miembros)
        #       hist[size] = hist.get(size, 0) + 1
        #
        hist = {}  # placeholder
        monitor.log_cluster_histogram(hist)
        monitor.log_peak("analisis_clusters", "fin_fase")

    monitor.gc_collect_with_log("post_histograma")

    # 5) Escritura de resultados (MARCXML, CSV, JSON)
    with monitor.phase("escritura_salida", "Escritura de MARCXML, CSV y JSON"):
        # >>> AQUÍ VA LA LÓGICA DE ESCRITURA DE TU v6_mini
        #
        #   escribir_marcxml(args.out, clusters_final, ...)
        #   escribir_csv(args.report, clusters_final, ...)
        #   escribir_json(args.clusters_json, clusters_final, ...)
        #
        monitor.log_peak("escritura_salida", "fin_fase")

    monitor.gc_collect_with_log("post_escritura")

    # 6) Snapshot final de tracemalloc
    monitor.log_tracemalloc_top("fin_ejecucion")


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    run_collective_with_monitor(args)


if __name__ == "__main__":
    main()
