from alma_bulk_tools.config import apply_cli_overrides, load_config


def test_apply_cli_overrides_ignores_nested_none_values() -> None:
    cfg = load_config(None)
    merged = apply_cli_overrides(
        cfg,
        {
            "download": {"artifacts": None, "max_workers": None},
            "runtime": {"max_runtime_min": None},
        },
    )
    assert merged["download"]["max_workers"] == 4
    assert merged["download"]["artifacts"] is None
