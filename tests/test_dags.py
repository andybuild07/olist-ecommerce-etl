import ast
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def _dag_files():
    return [p for p in DAGS_DIR.rglob("*.py") if not p.name.startswith("test_")]


def test_dag_files_exist():
    files = _dag_files()
    assert len(files) > 0, f"No DAG Python files found in {DAGS_DIR}"


def test_dag_files_have_valid_syntax():
    for path in _dag_files():
        source = path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(path))