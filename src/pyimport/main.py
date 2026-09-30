"""Entry point principal para pyimport con soporte multientorno y ejecución de scripts específicos:
  --env dev|qa|prod
  --script src_ingest_crops|src_load_crops|src_transform_crops|src_transform_consolidado
           src_ingest_sipsa|src_load_sipsa
           src_ingest_oni|src_load_oni
           src_ingest_ckan_comentarios|src_load_ckan_comentarios|src_transform_ckan_comentarios|all
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyimport.config_loader import load_config
from pyimport.src_ingest_crops import run_ingest_crops
from pyimport.src_load_crops import run_load_crops
from pyimport.src_transform_consolidado import run_transform_consolidado
from pyimport.src_ingest_sipsa import run_ingest_sipsa
from pyimport.src_load_sipsa import run_load_sipsa
from pyimport.src_ingest_oni import run_ingest_oni
from pyimport.src_load_oni import run_load_oni
from pyimport.src_ingest_ckan_comentarios import run_ingest_ckan_comentarios
from pyimport.src_load_ckan_comentarios import run_load_ckan_comentarios
from pyimport.src_transform_ckan_comentarios import run_transform_ckan_comentarios


from pyimport.src_orchestrator import run_orchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyimport",
        description="Pipeline Multientorno de Datos Agrícolas, SIPSA, ONI NOAA y CKAN comentarios.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Ruta al archivo de configuración config.yaml (default: subcarpeta config/ de los scripts)",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "qa", "prod"],
        default=None,
        help="Entorno a ejecutar: dev, qa, prod (default: definido en config.yaml)",
    )
    parser.add_argument(
        "--script",
        choices=[
            "src_orchestrator",
            "src_ingest_crops",
            "src_load_crops",
            "src_transform_crops",
            "src_transform_consolidado",
            "src_ingest_sipsa",
            "src_load_sipsa",
            "src_ingest_oni",
            "src_load_oni",
            "src_ingest_ckan_comentarios",
            "src_load_ckan_comentarios",
            "src_transform_ckan_comentarios",
            "all",
        ],
        default="all",
        help="Script específico a ejecutar (default: all).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    cfg = load_config(args.config, env=args.env)
    print(f"🚀 [MAIN] Ejecutando pyimport | Entorno: {cfg.get('active_env')} | Script: {args.script}")

    script = args.script
    if script == "src_orchestrator":
        run_orchestrator(cfg)
    elif script == "src_ingest_crops":
        run_ingest_crops(cfg)
    elif script == "src_load_crops":
        run_load_crops(cfg)
    elif script in ("src_transform_crops", "src_transform_consolidado"):
        run_transform_consolidado(cfg)
    elif script == "src_ingest_sipsa":
        run_ingest_sipsa(cfg)
    elif script == "src_load_sipsa":
        run_load_sipsa(cfg)
    elif script == "src_ingest_oni":
        run_ingest_oni(cfg)
    elif script == "src_load_oni":
        run_load_oni(cfg)
    elif script == "src_ingest_ckan_comentarios":
        run_ingest_ckan_comentarios(cfg)
    elif script == "src_load_ckan_comentarios":
        run_load_ckan_comentarios(cfg)
    elif script == "src_transform_ckan_comentarios":
        run_transform_ckan_comentarios(cfg)
    elif script == "all":
        print("🌐 === EJECUTANDO PIPELINE COMPLETO MULTIENTORNO ===")
        run_ingest_sipsa(cfg)
        run_load_sipsa(cfg)
        run_ingest_oni(cfg)
        run_load_oni(cfg)
        run_ingest_crops(cfg)
        run_load_crops(cfg)
        run_transform_consolidado(cfg)
        run_ingest_ckan_comentarios(cfg)
        run_load_ckan_comentarios(cfg)
        run_transform_ckan_comentarios(cfg)
        print("🎉 === PIPELINE PROCESADO EXITOSAMENTE ===")

    return 0


if __name__ == "__main__":
    sys.exit(main())
