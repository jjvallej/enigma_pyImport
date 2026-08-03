"""Entry point for pyimport."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyimport.sipsa import import_sipsa, parse_period_arg


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyimport",
        description=(
            "Descarga anexos mensuales SIPSA (DANE) y genera un CSV con "
            "anio, mes, alimento (columna A) y valor (columna J / precio Cali)."
        ),
    )
    parser.add_argument(
        "--start",
        default="2015-02",
        help="Periodo inicial YYYY-MM (default: 2015-02)",
    )
    parser.add_argument(
        "--end",
        default="2026-06",
        help="Periodo final YYYY-MM (default: 2026-06)",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directorio donde se guardan los Excel descargados",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sipsa_precios.csv"),
        help="Ruta del CSV consolidado",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    start = parse_period_arg(args.start)
    end = parse_period_arg(args.end)

    result = import_sipsa(
        start,
        end,
        download_dir=args.download_dir,
        output=args.output,
    )

    print(f"Archivos procesados: {len(result['files'])}")
    if result["missing"]:
        print(f"Periodos sin archivo: {', '.join(result['missing'])}")
    print(f"Registros escritos: {result['rows']}")
    print(f"CSV: {result['output']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
