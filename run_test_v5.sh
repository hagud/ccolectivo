#!/bin/bash

# =========================================================
#   RUN_TEST_V5.SH — Ejecutor de pruebas v6 (12 modos)
#   Bibliotecas: ISFODOSU, UNAE, UPN_MEXICO, kohaUNIPE
#   Autor: Hugo + ChatGPT
# =========================================================

# Colores para el menú
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
BLUE="\e[34m"
RESET="\e[0m"

# Carpetas
OUTDIR="tests_output"
SCRIPT="colectivo_v6_mini.py"

mkdir -p "$OUTDIR"

# ---------------------------------------------------------
# Función para ejecutar un modo
# ---------------------------------------------------------
run_mode() {
  local mode="$1"
  local extra="$2"

  echo -e "${BLUE}-----------------------------------------------------${RESET}"
  echo -e "${YELLOW} Ejecutando fase: $mode ${RESET}"
  echo -e "${BLUE}-----------------------------------------------------${RESET}"

  CMD="python3 $SCRIPT \
    --src LIB1=ISFODOSU.mrc \
    --src LIB2=UNAE.mrc \
    --src LIB3=UPN_MEXICO.mrc \
    --src LIB4=kohaUNIPE.mrc \
    --out $OUTDIR/${mode}.xml \
    --report $OUTDIR/${mode}.csv \
    --clusters-json $OUTDIR/${mode}.json \
    $extra"

  echo -e "${GREEN}▶ $CMD${RESET}"
  eval $CMD

  if [[ $? -ne 0 ]]; then
    echo -e "${RED}❌ Error ejecutando $mode${RESET}"
  else
    echo -e "${GREEN}✔ Finalizado: $mode${RESET}"
  fi

  echo
}

# ---------------------------------------------------------
# Menú
# ---------------------------------------------------------
while true; do
  clear
  echo -e "${GREEN}===============================================${RESET}"
  echo -e "${GREEN}       SELECCIÓN DE MODOS DE EJECUCIÓN         ${RESET}"
  echo -e "${GREEN}===============================================${RESET}"
  echo
  echo " 1) 01_strong_only"
  echo " 2) 02_fuzzy_safe"
  echo " 3) 03_fuzzy_standard"
  echo " 4) 04_fuzzy_aggressive"
  echo " 5) 05_prefer_fields"
  echo " 6) 06_alt_tags"
  echo " 7) 07_fast_nocheck"
  echo " 8) 08_keep9xx"
  echo " 9) 09_final"
  echo "10) 10_fuzzy_marc_97"
  echo "11) 11_fuzzy_marc_98"
  echo "12) 12_fuzzy_marc_99"
  echo
  echo " A) Ejecutar TODOS los modos (01 → 12)"
  echo " M) Parámetros manuales"
  echo " Q) Salir"
  echo
  read -p "👉 Elige opción: " opt

  case "$opt" in

    1) run_mode "01_strong_only"     "--no-fuzzy --strong-only" ;;
    2) run_mode "02_fuzzy_safe"      "--weak-threshold 90" ;;
    3) run_mode "03_fuzzy_standard"  "--weak-threshold 85" ;;
    4) run_mode "04_fuzzy_aggressive" "--weak-threshold 80" ;;
    5) run_mode "05_prefer_fields"   "--prefer-fields" ;;
    6) run_mode "06_alt_tags"        "--alt-tags" ;;
    7) run_mode "07_fast_nocheck"    "--fast" ;;
    8) run_mode "08_keep9xx"         "--keep9xx" ;;
    9) run_mode "09_final"           "--final-pipeline" ;;
    10) run_mode "10_fuzzy_marc_97"  "--weak-threshold 97" ;;
    11) run_mode "11_fuzzy_marc_98"  "--weak-threshold 98" ;;
    12) run_mode "12_fuzzy_marc_99"  "--weak-threshold 99" ;;

    A|a)
      echo -e "${YELLOW}===============================================${RESET}"
      echo -e "${YELLOW}     EJECUTANDO TODAS LAS FASES (01 → 12)      ${RESET}"
      echo -e "${YELLOW}===============================================${RESET}"

      run_mode "01_strong_only" "--no-fuzzy --strong-only"
      run_mode "02_fuzzy_safe" "--weak-threshold 90"
      run_mode "03_fuzzy_standard" "--weak-threshold 85"
      run_mode "04_fuzzy_aggressive" "--weak-threshold 80"
      run_mode "05_prefer_fields" "--prefer-fields"
      run_mode "06_alt_tags" "--alt-tags"
      run_mode "07_fast_nocheck" "--fast"
      run_mode "08_keep9xx" "--keep9xx"
      run_mode "09_final" "--final-pipeline"
      run_mode "10_fuzzy_marc_97" "--weak-threshold 97"
      run_mode "11_fuzzy_marc_98" "--weak-threshold 98"
      run_mode "12_fuzzy_marc_99" "--weak-threshold 99"
      ;;

    M|m)
      echo -e "${YELLOW}Introduce parámetros manuales:${RESET}"
      read -p "Comando extra: " manual
      run_mode "manual_run" "$manual"
      ;;

    Q|q)
      echo "Saliendo."
      exit 0
      ;;

    *)
      echo -e "${RED}❌ Opción no válida${RESET}"
      sleep 1
      ;;
  esac

  read -p "Pulsa ENTER para volver al menú..."
done
