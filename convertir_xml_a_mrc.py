#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Conversión robusta de MARCXML (.xml) a MARC binario (.mrc)
usando yaz-marcdump — procesamiento en streaming sin consumir RAM.

Requisitos:
    sudo apt install yaz
"""

import sys
import os
import subprocess

def convertir_xml(xml_path):
    base, ext = os.path.splitext(xml_path)
    if ext.lower() != ".xml":
        print("ERROR: El archivo no es XML:", xml_path)
        return

    mrc_path = base + ".mrc"
    print("Convirtiendo:", xml_path, "→", mrc_path)

    cmd = [
        "yaz-marcdump",
        "-i", "marcxml",
        "-o", "marc",
        xml_path
    ]

    try:
        with open(mrc_path, "wb") as fh_out:
            subprocess.run(cmd, stdout=fh_out, stderr=subprocess.PIPE, check=True)
        print("OK: Conversión completada:", mrc_path)
        print("")
    except subprocess.CalledProcessError as e:
        err = e.stderr.decode("utf-8", errors="ignore")
        print("ERROR al convertir XML → MRC:")
        print(err)
        print("")

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 convertir_xml_a_mrc.py fichero1.xml fichero2.xml ...")
        sys.exit(1)

    for xml_file in sys.argv[1:]:
        if os.path.isfile(xml_file):
            convertir_xml(xml_file)
        else:
            print("ADVERTENCIA: No existe el archivo:", xml_file)

if __name__ == "__main__":
    main()
