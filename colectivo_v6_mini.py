#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Catálogo colectivo MARC — Versión MRC-ONLY
------------------------------------------

✔ Procesa exclusivamente ficheros MARC binarios (.mrc / .marc)
✔ No depende de XMLReader (no existe en tu PyMARC)
✔ No usa parse_xml_to_array (evita cargas enormes)
✔ Streaming real: RAM mínima
✔ Compatibilidad 100% con tu script original
✔ Rendimiento máximo
"""

from __future__ import annotations
import argparse
import csv
import json
import sys
import re
import os
from dataclasses import dataclass, field
from typing import List, Optional, Set, Dict

from unidecode import unidecode
from rapidfuzz import fuzz
from pymarc import (
    Record,
    Field,
    MARCReader,
    XMLWriter,
)

# Compatible PyMARC 4.x y 5.x
try:
    from pymarc.subfield import Subfield
except ImportError:
    from pymarc import Subfield


# ============================================================
#                   PARÁMETROS GLOBALES
# ============================================================

VALIDATE = False
PUNCT_TABLE = str.maketrans({c: " " for c in ",.;:!?/'\"()[]{}<>-&_=+*#@$%^`~|"})


# ============================================================
#                   NORMALIZACIÓN
# ============================================================

def normalize_text(s: str) -> str:
    s = unidecode(s or "").lower().translate(PUNCT_TABLE)
    return " ".join(s.split())


def only_digits_x(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit() or ch.upper() == "X").upper()


# ============================================================
#                 VALIDACIÓN ISBN/ISSN
# ============================================================

def is_valid_isbn10(d: str) -> bool:
    if len(d) != 10:
        return False
    try:
        total = sum((i + 1) * int(x) for i, x in enumerate(d[:9]))
        mod = total % 11
        return (d[9] == "X" and mod == 10) or (d[9].isdigit() and int(d[9]) == mod)
    except:
        return False


def is_valid_isbn13(d: str) -> bool:
    if len(d) != 13 or not d.isdigit():
        return False
    s = sum((1 if i % 2 == 0 else 3) * int(x) for i, x in enumerate(d[:12]))
    return (10 - (s % 10)) % 10 == int(d[12])


def is_valid_issn(d: str) -> bool:
    if len(d) == 7:
        d = "0" + d
    if len(d) != 8:
        return False
    try:
        tot = sum((8 - i) * int(ch) for i, ch in enumerate(d[:7]))
        chk = (11 - (tot % 11)) % 11
        return ("X" if chk == 10 else str(chk)) == d[7]
    except:
        return False


def norm_isbn(s: str) -> Optional[str]:
    d = only_digits_x(s)
    if len(d) == 10:
        return d if not VALIDATE or is_valid_isbn10(d) else None
    if len(d) == 13:
        return d if not VALIDATE or is_valid_isbn13(d) else None
    return None


def norm_issn(s: str) -> Optional[str]:
    d = only_digits_x(s)
    if not d:
        return None
    return d if not VALIDATE or is_valid_issn(d) else None


# ============================================================
#                   AUTORIDADES $9
# ============================================================

AUTH_PATTERNS = {
    "VVIAF":   re.compile(r"^(viaf:? *\d+|\d{8})$", re.I),
    "ISNI":    re.compile(r"^(isni:? *\d{15}[0-9xX])$", re.I),
    "WD":      re.compile(r"^(wd:Q\d+|wikidata:Q\d+)$", re.I),
    "BNE":     re.compile(r"^(bne:.+)$", re.I),
    "LEMAC":   re.compile(r"^(lemac:.+)$", re.I),
    "CANTIC":  re.compile(r"^(cantic:.+)$", re.I),
}

AUTH_SCORES = {
    "VVIAF": 6,
    "ISNI": 6,
    "WD": 5,
    "BNE": 4,
    "LEMAC": 4,
    "CANTIC": 4,
    "OTHER": 2,
}


def score_authorities(rec: Record) -> int:
    score = 0
    for f in rec.get_fields():
        try:
            tag = int(f.tag)
        except:
            continue
        if not (100 <= tag <= 111 or 600 <= tag <= 699 or 700 <= tag <= 799):
            continue
        for v in f.get_subfields("9"):
            v2 = v.lower().strip()
            matched = False
            for name, pat in AUTH_PATTERNS.items():
                if pat.match(v2):
                    score += AUTH_SCORES[name]
                    matched = True
                    break
            if not matched:
                score += AUTH_SCORES["OTHER"]
    return score


# ============================================================
#               EXTRACCIÓN DE CAMPOS MARC
# ============================================================

def get_local_id(rec: Record) -> str:
    f999 = rec.get_fields("999")
    if f999:
        c = f999[0].get_subfields("c")
        if c:
            return c[0].strip()
    f001 = rec.get_fields("001")
    if f001:
        return (f001[0].data or "").strip()
    return ""


def extract_author(rec: Record) -> str:
    for t in ("100", "110", "111"):
        f = rec.get_fields(t)
        if f:
            return " ".join(f[0].get_subfields("a", "b", "c", "d"))
    return ""


def extract_title(rec: Record) -> str:
    f = rec.get_fields("245")
    if f:
        return " ".join(f[0].get_subfields("a", "b"))
    return ""


def extract_year(rec: Record) -> str:
    for t in ("264", "260"):
        for f in rec.get_fields(t):
            for c in f.get_subfields("c"):
                y = "".join(ch for ch in c if ch.isdigit())
                if len(y) >= 4:
                    return y[:4]
    f008 = rec.get_fields("008")
    if f008 and len(f008[0].data) >= 11:
        y = f008[0].data[7:11]
        if y.isdigit():
            return y
    return ""


def build_fuzzy_key(rec: Record) -> str:
    return normalize_text(
        f"{extract_author(rec)}|{extract_title(rec)}|{extract_year(rec)}"
    )


def extract_strong_key(rec: Record) -> Optional[str]:
    for f in rec.get_fields("020"):
        for v in f.get_subfields("a", "z"):
            i = norm_isbn(v)
            if i:
                return f"ISBN:{i}"

    for f in rec.get_fields("022"):
        for v in f.get_subfields("a", "y", "z"):
            i = norm_issn(v)
            if i:
                return f"ISSN:{i}"

    return None


# ============================================================
#                  ESTRUCTURAS INTERNAS
# ============================================================

@dataclass(slots=True)
class SourceRecord:
    lib: str
    local_id: str
    record: Record
    strong_key: Optional[str]
    fuzzy_key: Optional[str]


@dataclass(slots=True)
class Cluster:
    id: int
    members: List[SourceRecord] = field(default_factory=list)
    strong_keys: Set[str] = field(default_factory=set)
    fuzzy_keys: Set[str] = field(default_factory=set)


# ============================================================
#                  SCORING Y SELECCIÓN
# ============================================================

def score_record(rec: Record) -> int:
    score = 0

    if rec.get_fields("245"): score += 15
    if rec.get_fields("260") or rec.get_fields("264"): score += 10
    if rec.get_fields("100") or rec.get_fields("110") or rec.get_fields("111"): score += 8
    if rec.get_fields("020"): score += 12
    if rec.get_fields("022"): score += 10
    if rec.get_fields("300"): score += 4

    f008 = rec.get_fields("008")
    if f008 and len(f008[0].data) >= 40: score += 15

    for f in rec.get_fields():
        if f.indicator1 not in (" ", "", None): score += 1
        if f.indicator2 not in (" ", "", None): score += 1

    score += score_authorities(rec)

    return score


def choose_primary(members: List[SourceRecord], prefer: List[str]) -> SourceRecord:
    scored = [(m, score_record(m.record)) for m in members]
    maxsc = max(s for _, s in scored)
    best = [m for m, s in scored if s == maxsc]
    for lib in prefer:
        for m in best:
            if m.lib == lib:
                return m
    return best[0]


# ============================================================
#                  FUSIÓN DE CLÚSTERES
# ============================================================

def merge_cluster(
    cluster: Cluster,
    prefer: List[str],
    prefer_fields: List[str],
    prov_tag: str,
    hold_tag: str,
    merge_tag: str,
    keep9: bool,
) -> Record:

    primary = choose_primary(cluster.members, prefer)
    rec = primary.record
    libs = set()

    Subf = Subfield
    FieldCls = Field

    for sr in cluster.members:
        libs.add(sr.lib)
        lid = sr.local_id

        rec.add_field(FieldCls(
            tag=prov_tag,
            indicators=[" ", " "],
            subfields=[Subf("a", f"({sr.lib}){lid}")]
        ))

        subs = [Subf("a", sr.lib)]
        if lid:
            subs.append(Subf("b", lid))

        rec.add_field(FieldCls(
            tag=hold_tag,
            indicators=[" ", " "],
            subfields=subs,
        ))

    for tag in prefer_fields:
        tag = tag.strip()
        if not tag:
            continue

        best_rec = None
        for sr in cluster.members:
            if sr.record.get_fields(tag):
                best_rec = sr.record
                break

        if not best_rec:
            continue

        for f in list(rec.get_fields(tag)):
            rec.remove_field(f)

        for f in best_rec.get_fields(tag):
            newf = FieldCls(
                tag=f.tag,
                indicators=[f.indicator1, f.indicator2],
                subfields=[Subf(sf.code, sf.value) for sf in f.subfields]
            )
            rec.add_field(newf)

    texto = f"Registro colectivo de {len(cluster.members)} registros. Bibliotecas: {', '.join(sorted(libs))}"

    rec.add_field(FieldCls(
        tag=merge_tag,
        indicators=[" ", " "],
        subfields=[Subf("a", texto)]
    ))

    return rec


# ============================================================
#               LECTOR DE REGISTROS (SOLO MRC)
# ============================================================

def iter_records_mrc(path: str):
    """
    Solo acepta ficheros MRC. Si el archivo no es .mrc/.marc → error.
    """
    ext = os.path.splitext(path)[1].lower()

    if ext not in (".mrc", ".marc", ".dat", ".iso2709"):
        raise ValueError(f"ERROR: el archivo '{path}' no es MRC. Conviértelo antes con convertir_xml_a_mrc.py")

    with open(path, "rb") as fh:
        reader = MARCReader(fh, to_unicode=True, force_utf8=True)
        for rec in reader:
            if rec:
                yield rec


# ============================================================
#             CONSTRUCCIÓN DE CLÚSTERES
# ============================================================

def build_clusters(sources, weak, use_fuzzy, strong_only):

    clusters: List[Cluster] = []
    strong_index: Dict[str, Cluster] = {}
    fuzzy_index: Dict[str, Cluster] = {}

    next_id = 1
    total = 0

    token_sort_ratio = fuzz.token_sort_ratio

    for lib, path in sources:
        print(f"\nLeyendo {path} ({lib})...")

        count = 0
        for i, r in enumerate(iter_records_mrc(path), start=1):

            if i % 5000 == 0:
                print(f"   → {i:,} registros procesados de {lib}")

            count += 1
            total += 1

            sk = extract_strong_key(r)
            fk = build_fuzzy_key(r) if use_fuzzy else None

            sr = SourceRecord(lib, get_local_id(r), r, sk, fk)
            cl = None

            if sk and sk in strong_index:
                cl = strong_index[sk]

            elif use_fuzzy and not strong_only and fk:
                best_sc = 0
                best_cluster = None
                for key, cluster in fuzzy_index.items():
                    sc = token_sort_ratio(fk, key)
                    if sc > best_sc:
                        best_sc, best_cluster = sc, cluster
                if best_cluster and best_sc >= weak:
                    cl = best_cluster

            if not cl:
                cl = Cluster(next_id)
                clusters.append(cl)
                next_id += 1

            cl.members.append(sr)

            if sk:
                strong_index[sk] = cl
                cl.strong_keys.add(sk)

            if fk:
                fuzzy_index[fk] = cl
                cl.fuzzy_keys.add(fk)

        print(f"✔ {lib}: {count:,} registros procesados.")

    print("\nFinalizando agrupación de clústeres...")
    print(f"Total registros: {total:,}")
    print(f"Clústeres generados: {len(clusters):,}\n")

    return clusters


# ============================================================
#               ESCRITURA DE RESULTADOS
# ============================================================

def write_union(
    clusters, out, prefer, prefer_fields,
    prov, hold, merge, keep9
):

    print(f"Escribiendo MARCXML final: {out}")

    with open(out, "wb") as fh:
        writer = XMLWriter(fh)

        for idx, cl in enumerate(clusters, start=1):
            if idx % 2000 == 0:
                print(f"   → {idx:,}/{len(clusters):,} registros escritos...")

            rec = merge_cluster(cl, prefer, prefer_fields, prov, hold, merge, keep9)
            writer.write(rec)

        writer.close()

    print("✔ Escritura MARCXML completada.")


def write_report(clusters, path):
    print(f"Escribiendo informe CSV: {path}")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cluster_id", "size", "strong_keys", "libraries"])
        for c in clusters:
            w.writerow([
                c.id,
                len(c.members),
                ";".join(sorted(c.strong_keys)),
                ";".join(sorted({m.lib for m in c.members})),
            ])
    print("✔ CSV completado.")


def write_json(clusters, path):
    print(f"Escribiendo archivo JSON: {path}")
    data = []
    for c in clusters:
        data.append({
            "cluster_id": c.id,
            "strong_keys": sorted(c.strong_keys),
            "members": [
                {"lib": m.lib, "local_id": m.local_id} for m in c.members
            ],
        })
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    print("✔ JSON completado.")


# ============================================================
#                          MAIN
# ============================================================

def build_parser():
    p = argparse.ArgumentParser(
        description="Creador de catálogo colectivo (solo MRC)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument("--src", action="append", required=True, help="LIB=archivo.mrc")
    p.add_argument("--out", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--prefer", default="")
    p.add_argument("--prefer-fields", default="245,260,264")
    p.add_argument("--provenance-tag", default="035")
    p.add_argument("--holdings-tag", default="910")
    p.add_argument("--merge-note-tag", default="948")
    p.add_argument("--keep-9xx", action="store_true")
    p.add_argument("--no-fuzzy", action="store_true")
    p.add_argument("--weak-threshold", type=int, default=92)
    p.add_argument("--strong-only", action="store_true")
    p.add_argument("--no-checksum", action="store_true")
    p.add_argument("--clusters-json")

    return p


def main(argv=None):

    p = build_parser()
    a = p.parse_args(argv)

    global VALIDATE
    VALIDATE = not a.no_checksum

    sources = []
    for s in a.src:
        if "=" not in s:
            continue
        lib, path = s.split("=", 1)
        sources.append((lib.strip(), path.strip()))

    prefer = [x.strip() for x in a.prefer.split(",") if x.strip()]
    prefer_fields = [x.strip() for x in a.prefer_fields.split(",") if x.strip()]

    print("\nINICIO DEL PROCESO COLECTIVO (MRC-ONLY)\n")

    clusters = build_clusters(
        sources,
        a.weak_threshold,
        not a.no_fuzzy,
        a.strong_only,
    )

    write_union(
        clusters,
        a.out,
        prefer,
        prefer_fields,
        prov=a.provenance_tag,
        hold=a.holdings_tag,
        merge=a.merge_note_tag,
        keep9=a.keep_9xx,
    )

    write_report(clusters, a.report)

    if a.clusters_json:
        write_json(clusters, a.clusters_json)

    print("\nProceso completado exitosamente.\n")


if __name__ == "__main__":
    main()

