from alma_bulk_tools.archive_query import build_adql_query


def test_build_adql_query_band_filters_match_numeric_band_list() -> None:
    adql = build_adql_query(
        start="2024-01-01",
        end="2024-02-01",
        date_field="release",
        filters={
            "bands_include": ["9"],
            "bands_exclude": ["10"],
            "exclude_tp": False,
            "exclude_7m": False,
            "project_codes_include": [],
            "project_codes_exclude": [],
            "min_freq_ghz": None,
            "max_freq_ghz": None,
        },
    )
    assert "band_list = '9'" in adql
    assert "NOT (band_list = '10'" in adql


def test_build_adql_query_array_filters_use_antenna_arrays() -> None:
    adql = build_adql_query(
        start="2024-01-01",
        end="2024-02-01",
        date_field="release",
        filters={
            "bands_include": [],
            "bands_exclude": [],
            "exclude_tp": True,
            "exclude_7m": True,
            "project_codes_include": [],
            "project_codes_exclude": [],
            "min_freq_ghz": None,
            "max_freq_ghz": None,
        },
    )
    assert "LOWER(antenna_arrays) NOT LIKE '%pm%'" in adql
    assert "LOWER(antenna_arrays) NOT LIKE '%cm%'" in adql
