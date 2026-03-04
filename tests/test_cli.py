from pathlib import Path

from alma_bulk_tools import cli
from alma_bulk_tools.models import MousRecord


def test_discover_cli_writes_editable_candidate_text(monkeypatch, tmp_path: Path, capsys) -> None:
    out_path = tmp_path / "candidates.txt"

    def fake_discover_mous(**kwargs):
        assert kwargs["start"] == "2024-01-01"
        assert kwargs["end"] == "2024-02-01"
        return (
            [
                MousRecord(
                    project_code="2024.1.00001.S",
                    member_ous_uid="uid://A001/X1/X2",
                    group_ous_uid="uid://A001/X1/X3",
                    eb_uids=["uid://A002/X100/X200"],
                    band_list=["BAND 6"],
                    release_date="2024-01-15",
                    obs_date="2023-12-20",
                    qa2_passed=True,
                    archive_meta={
                        "sb_name": "SB_A",
                        "science_category": "Galaxy evolution",
                        "execution_count": 1,
                        "spw_count": 2,
                        "min_spw_total_width_mhz": 937.5,
                        "max_spw_total_width_mhz": 2000.0,
                        "min_nchan": 960,
                        "max_nchan": 3840,
                        "array": "12m+TP",
                        "max_baseline_m": 1850.0,
                        "science_target_count": 1,
                        "is_mosaic": False,
                        "qa2_status": "PASS",
                    },
                )
            ],
            "SELECT * FROM ivoa.obscore",
        )

    monkeypatch.setattr(cli, "discover_mous", fake_discover_mous)

    rc = cli.main(
        [
            "discover",
            "--start",
            "2024-01-01",
            "--end",
            "2024-02-01",
            "--out",
            str(out_path),
        ]
    )

    captured = capsys.readouterr()
    text = out_path.read_text(encoding="utf-8")

    assert rc == 0
    assert f"Wrote 1 candidates to {out_path}" in captured.out
    assert text.startswith("# alma-bulk discover candidates\n")
    assert "project_code,science_category,mous_uid,sb_name" in text
    assert "2024.1.00001.S,Galaxy evolution,uid://A001/X1/X2,SB_A,1,2,BAND 6,937.5 MHz,2 GHz,960,3840,12m+TP,1850,1,N,PASS,2023-12-20,2024-01-15,uid://A001/X1/X3,,uid://A002/X100/X200" in text


def test_sample_cli_writes_selected_candidates_and_report(tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "candidates.txt"
    output_path = tmp_path / "sampled.txt"
    supplemental_path = tmp_path / "sampled.supplemental.txt"
    report_dir = tmp_path / "report"
    input_path.write_text(
        "\n".join(
            [
                "# alma-bulk discover candidates",
                "project_code,science_category,mous_uid,sb_name,executions,spws,band,min_spw_total_width,max_spw_total_width,min_nchan,max_nchan,array,max_baseline_m,science_targets,is_mosaic,qa2_status,observation_date,delivery_date,group_ous_uid,science_goal_uid,eb_uids",
                "2024.1.00001.S,Galaxy evolution,uid://A001/X1/X2,SB_A,1,2,BAND 6,937.5 MHz,2 GHz,960,3840,12m,1850,1,N,PASS,2023-12-20,2024-01-15,uid://A001/X1/X3,,uid://A002/X100/X200",
                "2024.1.00002.S,Stars and stellar evolution,uid://A001/X1/X4,SB_B,1,2,BAND 3,120 MHz,1.875 GHz,492,1920,7m,55,1,N,PASS,2023-12-21,2024-01-15,uid://A001/X1/X5,,uid://A002/X100/X201",
                "2024.1.00003.S,Active galaxies,uid://A001/X1/X6,SB_C,1,2,BAND 7,1.875 GHz,2 GHz,1920,1920,12m,3600,1,N,PASS,2023-12-22,2024-01-15,uid://A001/X1/X7,,uid://A002/X100/X202",
                "2024.1.00004.S,Galaxy evolution,uid://A001/X1/X8,SB_D,1,2,BAND 6,937.5 MHz,2 GHz,960,3840,TP,300,1,N,PASS,2023-12-23,2024-01-15,uid://A001/X1/X9,,uid://A002/X100/X203",
                "",
            ]
        ),
        encoding="utf-8",
    )

    rc = cli.main(
        [
            "sample",
            "--input",
            str(input_path),
            "--out",
            str(output_path),
            "--report-dir",
            str(report_dir),
            "--target-size",
            "2",
            "--seed",
            "7",
            "--max-per-project",
            "1",
        ]
    )

    captured = capsys.readouterr()
    sample_text = output_path.read_text(encoding="utf-8")
    supplemental_text = supplemental_path.read_text(encoding="utf-8")
    report_html = (report_dir / "coverage_report.html").read_text(encoding="utf-8")
    sample_rows = [
        line for line in sample_text.splitlines()
        if line and not line.startswith("#") and not line.startswith("project_code,")
    ]
    supplemental_rows = [
        line for line in supplemental_text.splitlines()
        if line and not line.startswith("#") and not line.startswith("project_code,")
    ]

    assert rc == 0
    assert "Sampled 2/4 records" in captured.out
    assert len(sample_rows) == 2
    assert "<h2>science_category vs band</h2>" in report_html
    assert "<h2>science_category vs max_baseline_bin</h2>" in report_html
    assert "selected/population" in report_html
    assert len(supplemental_rows) == 1
    assert ",TP," not in supplemental_text
