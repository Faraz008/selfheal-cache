"""Fail if docs reference tests or files that do not exist."""

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]


def test_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    return {
        n.name
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test_")
    }


def main() -> int:
    problems: list[str] = []
    cache: dict[Path, set[str]] = {}
    for doc in DOCS:
        text = doc.read_text()
        for m in re.finditer(r"`(tests/[\w/]+\.py)::(test_\w+)`", text):
            f, name = ROOT / m.group(1), m.group(2)
            if not f.exists():
                problems.append(f"{doc.name}: missing file {m.group(1)}")
                continue
            names = cache.setdefault(f, test_names(f))
            if name not in names:
                problems.append(f"{doc.name}: {m.group(1)} has no test named {name}")
        for m in re.finditer(r"!?\[[^\]]*\]\(([^)#\s]+)\)", text):  # relative links and images
            target = m.group(1)
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            if not (doc.parent / target).resolve().exists():
                problems.append(f"{doc.name}: broken link {target}")
        for m in re.finditer(r"`(docs/[\w./-]+\.(?:png|csv|txt|md|cast|json))`", text):
            if not (ROOT / m.group(1)).exists():
                problems.append(f"{doc.name}: missing artifact {m.group(1)}")
    for p in problems:
        print("FAIL", p)
    print(f"checked {len(DOCS)} docs, {len(problems)} problem(s)")
    return 1 if problems else 0


sys.exit(main())
