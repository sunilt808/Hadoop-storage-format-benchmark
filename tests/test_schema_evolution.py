"""
Unit tests for Schema Evolution Tester.
"""

import pytest
from pathlib import Path
from src.schema_evolution import SchemaEvolutionTester

def test_schema_evolution_suite(tmp_path):
    tester = SchemaEvolutionTester(tmp_path)
    res = tester.run_all_evolution_tests()
    assert res["summary_count"] >= 10
    assert Path(res["matrix_path"]).exists()
