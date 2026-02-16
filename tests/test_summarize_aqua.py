from pathlib import Path

from alma_bulk_tools.summarize import parse_aqua_report, parse_flag_template, summarize_mous
from alma_bulk_tools.utils import atomic_write_json


def test_parse_aqua_report_extracts_topics_and_stages(tmp_path: Path) -> None:
    xml = """
    <Root>
      <QaPerTopic Topic="Calibration" Score="Pass" Reason="All good" />
      <QaPerTopic Topic="Imaging" Score="Fail" Reason="High noise" />
      <QaPerStage Name="bandpass" Score="Suboptimal" Reason="Low SNR" />
      <RepresentativeScore Name="applycal" Score="Pass" Reason="ok" />
    </Root>
    """
    p = tmp_path / "pipeline_aquareport.xml"
    p.write_text(xml, encoding="utf-8")

    parsed = parse_aqua_report(p)
    assert len(parsed["qa_per_topic"]) == 2
    assert any(item.get("topic") == "Calibration" for item in parsed["qa_per_topic"])
    assert any(item.get("name") == "bandpass" for item in parsed["qa_per_stage"])
    assert "High noise" in parsed["qa_reasons"]


def test_parse_flag_template_counts_manual_and_reasons(tmp_path: Path) -> None:
    text = """
    # reason: remove obvious RFI
    flagdata(vis='uid', mode='manual', antenna='DV01')
    flagdata(vis='uid', mode='clip', clipminmax=[0,1])
    flagdata(vis='uid', mode='manual', reason='shadowing')
    """
    p = tmp_path / "uid___x.flagtemplate.txt"
    p.write_text(text, encoding="utf-8")

    parsed = parse_flag_template(p)
    assert parsed["dr_flag_commands_count"] == 3
    assert parsed["dr_manual_flag_commands_count"] == 2
    assert parsed["dr_action_evidence"] is True
    assert parsed["dr_intervention_suspected"] is True
    reasons = {r["reason"] for r in parsed["flag_reasons"]}
    assert "shadowing" in reasons


def test_summarize_builds_run_level_summary(tmp_path: Path) -> None:
    mous_dir = tmp_path / "member.uid___A001_X1_X2"
    delivered = mous_dir / "delivered"
    delivered.mkdir(parents=True)
    (delivered / "uid___A002_X123.qa2_report.pdf").write_bytes(b"%PDF-1.4\n")
    (delivered / "script" / "member.uid___A001_X1_X2.hifa_calimage.pprequest.xml").parent.mkdir(
        parents=True
    )
    (delivered / "script" / "member.uid___A001_X1_X2.hifa_calimage.pprequest.xml").write_text(
        "<SciPipeRequest/>", encoding="utf-8"
    )

    manifest_path = mous_dir / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A001/X1/X2",
            "project_code": "2024.1.00001.S",
            "band_list": ["BAND 6"],
            "qa2_passed": True,
            "artifacts": [],
        },
    )

    summary = summarize_mous(
        mous_dir=mous_dir,
        manifest_path=manifest_path,
        query_timestamp="2026-02-15T00:00:00Z",
        tool_version="0.1.0",
        write_markdown=False,
    )

    assert summary["schema_version"] == 2
    assert summary["qa"]["qa2_status"] == "PASS"
    assert summary["has_delivered_products"] is True
    assert summary["has_run1_products"] is False
    assert len(summary["runs"]["delivered"]["qa_report_files"]) == 1


def test_summarize_infers_qa0_from_run_membership(tmp_path: Path) -> None:
    mous_dir = tmp_path / "member.uid___A001_X1_X2"
    delivered = mous_dir / "delivered" / "calibration"
    run1 = mous_dir / "run1" / "calibration"
    delivered.mkdir(parents=True)
    run1.mkdir(parents=True)

    (delivered / "uid___A002_X100_X200.flagtemplate.txt").write_text(
        "flagdata(vis='uid', mode='manual', reason='shadow')\n", encoding="utf-8"
    )
    (run1 / "uid___A002_X101_X201.flagtemplate.txt").write_text(
        "flagdata(vis='uid', mode='clip')\n", encoding="utf-8"
    )

    manifest_path = mous_dir / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A001/X1/X2",
            "project_code": "2024.1.00001.S",
            "eb_uids": ["uid://A002/X100/X200", "uid://A002/X101/X201"],
            "artifacts": [],
        },
    )

    summary = summarize_mous(
        mous_dir=mous_dir,
        manifest_path=manifest_path,
        query_timestamp="2026-02-15T00:00:00Z",
        tool_version="0.1.0",
        write_markdown=False,
    )

    eb_map = {item["eb_uid"]: item for item in summary["qa"]["eb_in_asa"]}
    assert eb_map["uid://A002/X100/X200"]["qa0_status"] == "PASS"
    assert eb_map["uid://A002/X100/X200"]["qa0_status_suggested"] is True
    assert eb_map["uid://A002/X101/X201"]["qa0_status"] == "SEMIPASS"
    assert eb_map["uid://A002/X101/X201"]["qa0_status_suggested"] is True
