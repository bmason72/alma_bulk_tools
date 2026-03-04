from alma_bulk_tools.archive_query import build_adql_query, group_rows_to_mous


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


def test_group_rows_to_mous_aggregates_discovery_metadata() -> None:
    rows = [
        {
            "proposal_id": "2024.1.00001.S",
            "member_ous_uid": "uid://A001/X1/X2",
            "group_ous_uid": "uid://A001/X1/X3",
            "asdm_uid": "uid://A002/X100/X200",
            "band_list": "6",
            "obs_release_date": "2024-03-10",
            "t_min": "60300.0",
            "qa2_passed": "T",
            "target_name": "Target A",
            "antenna_arrays": "DV01:DV10",
            "schedblock_name": "uid___A001_X1_X2_C36-5",
            "frequency_support": "[100.0..101.0GHz, 488.281kHz, 1920, XX] U [102.0..103.0GHz, 976.562kHz, 960, XX]",
            "science_observation": "T",
            "is_mosaic": "F",
            "frequency": "101.5",
            "spatial_resolution": "0.50",
        },
        {
            "proposal_id": "2024.1.00001.S",
            "member_ous_uid": "uid://A001/X1/X2",
            "group_ous_uid": "uid://A001/X1/X3",
            "asdm_uid": "uid://A002/X101/X201",
            "band_list": "6",
            "obs_release_date": "2024-03-11",
            "t_min": "60301.0",
            "qa2_passed": "T",
            "target_name": "Target B",
            "antenna_arrays": "CM01:CM10 PM01:PM03",
            "schedblock_name": "uid___A001_X1_X2_C36-5",
            "frequency_support": "[104.0..105.0GHz, 244.141kHz, 3840, XX]",
            "science_observation": "T",
            "is_mosaic": "T",
            "frequency": "104.5",
            "spatial_resolution": "0.30",
        },
    ]

    records = group_rows_to_mous(rows, filters={})

    assert len(records) == 1
    record = records[0]
    assert record.archive_meta["sb_name"] == "uid___A001_X1_X2_C36-5"
    assert record.archive_meta["execution_count"] == 2
    assert record.archive_meta["spw_count"] == 3
    assert record.archive_meta["min_spw_total_width_mhz"] == 1000.0
    assert record.archive_meta["max_spw_total_width_mhz"] == 1000.0
    assert record.archive_meta["min_nchan"] == 960
    assert record.archive_meta["max_nchan"] == 3840
    assert record.archive_meta["array"] == "12m+7m+TP"
    assert round(record.archive_meta["max_baseline_m"], 1) == 1972.5
    assert record.archive_meta["science_target_count"] == 2
    assert record.archive_meta["is_mosaic"] is True
    assert record.archive_meta["qa2_status"] == "PASS"


def test_group_rows_to_mous_infers_nchan_from_bandwidth_and_channel_width() -> None:
    rows = [
        {
            "proposal_id": "2024.1.00001.S",
            "member_ous_uid": "uid://A001/X1/X9",
            "group_ous_uid": "uid://A001/X1/X8",
            "asdm_uid": "uid://A002/X100/X999",
            "band_list": "6",
            "obs_release_date": "2024-03-10",
            "t_min": "60300.0",
            "qa2_passed": "T",
            "target_name": "Target A",
            "antenna_arrays": "DV01:DV10",
            "schedblock_name": "SB",
            "frequency_support": "[100.0..101.875GHz, 488.281kHz, XX]",
            "science_observation": "T",
            "is_mosaic": "F",
            "frequency": "101.0",
            "spatial_resolution": "0.50",
        }
    ]

    records = group_rows_to_mous(rows, filters={})

    assert len(records) == 1
    record = records[0]
    assert record.archive_meta["min_spw_total_width_mhz"] == 1875.0
    assert record.archive_meta["min_nchan"] == 3840
    assert record.archive_meta["max_nchan"] == 3840
