from pathlib import Path


def test_scaffold_integrity():
    assert Path("docs/prd/prd.md").exists()
    assert Path("plans/next-enhancements.md").exists()
    assert Path("plans/focus.md").exists()
    assert Path("plans/cycle_state.json").exists()
