"""Genera datos de prueba de comentarios CKAN para staging (sin Postgres).

Uso:
  python scripts/generate_ckan_comentarios_fixtures.py
  python scripts/generate_ckan_comentarios_fixtures.py --n 1000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pyimport.src_ingest_ckan_comentarios import (  # noqa: E402
    generate_comment_records,
    write_comment_fixtures,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Genera fixtures de comentarios CKAN")
    parser.add_argument("--n", type=int, default=1000, help="Número de registros (default 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/raw/ckan_comentarios"),
        help="Directorio staging local",
    )
    args = parser.parse_args(argv)

    rows = generate_comment_records(args.n, seed=args.seed)
    paths = write_comment_fixtures(rows, args.out_dir)

    from collections import Counter

    hints = Counter(r["fixture_sentiment_hint"] for r in rows)
    print(f"✅ Generados {len(rows)} comentarios en {args.out_dir}")
    print(f"   Archivos: {len(paths)}")
    for p in paths:
        print(f"   - {p.name}")
    print(f"   Distribución hint POS/NEG/NEU: {dict(hints)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
