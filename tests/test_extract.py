"""
Tests for petey-web schema loading.

Core petey library tests (extract_text, build_model, provider detection, etc.)
live in the petey repo. This file only tests web-specific schema integration.
"""
from pathlib import Path

import pytest

from petey import load_schema

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"


class TestLoadSchema:
    def test_loads_par_schema(self):
        par_path = SCHEMAS_DIR / "par_decision.yaml"
        if not par_path.exists():
            pytest.skip("par_decision.yaml not found")
        model, spec = load_schema(par_path)
        assert spec["name"] == "PAR Decision"
        assert "petitioner" in spec["fields"]
