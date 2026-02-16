import io
import tarfile
from pathlib import Path

from alma_bulk_tools.unpack import unpack_mous_delivered
from alma_bulk_tools.utils import atomic_write_json


def _write_tar(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def _tar_bytes(members: dict[str, bytes]) -> bytes:
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as tar:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    return data.getvalue()


def test_unpack_auxiliary_archive_in_place_and_remove_tar(tmp_path: Path) -> None:
    delivered = tmp_path / "member.uid___X" / "delivered"
    delivered.mkdir(parents=True)

    aux = delivered / "auxproducts.tgz"
    _write_tar(aux, {"qa/pipeline_aquareport.xml": b"<Root/>"})

    manifest_path = delivered.parent / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A/B/C",
            "project_code": "P",
            "artifacts": [
                {
                    "kind": "auxiliary",
                    "filename": aux.name,
                    "local_path": str(aux),
                    "status": "present",
                }
            ],
        },
    )

    manifest = unpack_mous_delivered(delivered, manifest_path)

    assert (delivered / "qa" / "pipeline_aquareport.xml").exists()
    assert not aux.exists()
    assert not (delivered / "extracted").exists()
    art = manifest["artifacts"][0]
    assert art.get("archive_removed_after_unpack") is True
    assert art.get("unpacked_to") == str(delivered)


def test_unpack_defaults_do_not_unpack_non_aux_archives(tmp_path: Path) -> None:
    delivered = tmp_path / "member.uid___X" / "delivered"
    delivered.mkdir(parents=True)

    script_tar = delivered / "scripts.tgz"
    _write_tar(script_tar, {"scripts/ScriptForPI.py": b"print('ok')\n"})

    manifest_path = delivered.parent / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A/B/C",
            "project_code": "P",
            "artifacts": [
                {
                    "kind": "scripts",
                    "filename": script_tar.name,
                    "local_path": str(script_tar),
                    "status": "present",
                }
            ],
        },
    )

    unpack_mous_delivered(delivered, manifest_path)

    assert script_tar.exists()
    assert not (delivered / "scripts" / "ScriptForPI.py").exists()


def test_unpack_readme_archives_by_default(tmp_path: Path) -> None:
    delivered = tmp_path / "member.uid___X" / "delivered"
    delivered.mkdir(parents=True)

    readme_tar = delivered / "README_bundle.tgz"
    _write_tar(readme_tar, {"README/README.txt": b"hello\n"})

    manifest_path = delivered.parent / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A/B/C",
            "project_code": "P",
            "artifacts": [
                {
                    "kind": "readme",
                    "filename": readme_tar.name,
                    "local_path": str(readme_tar),
                    "status": "present",
                }
            ],
        },
    )

    unpack_mous_delivered(delivered, manifest_path)

    assert (delivered / "README" / "README.txt").exists()
    assert not readme_tar.exists()


def test_unpack_strips_redundant_asa_prefix_and_backfills_science_goal(tmp_path: Path) -> None:
    delivered = tmp_path / "member.uid___X" / "delivered"
    delivered.mkdir(parents=True)
    aux = delivered / "outer_aux.tar"

    inner_path = (
        "2022.1.00055.S/science_goal.uid___A001_X2d20_X3ca0/"
        "group.uid___A001_X2d20_X3ca1/member.uid___A001_X2d20_X3ca2/"
        "calibration/file.txt"
    )
    _write_tar(aux, {inner_path: b"ok\n"})

    manifest_path = delivered.parent / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A001/X2d20/X3ca2",
            "project_code": "2022.1.00055.S",
            "science_goal_uid": None,
            "artifacts": [
                {
                    "kind": "auxiliary",
                    "filename": aux.name,
                    "local_path": str(aux),
                    "status": "present",
                }
            ],
        },
    )

    manifest = unpack_mous_delivered(delivered, manifest_path)
    assert (delivered / "calibration" / "file.txt").exists()
    assert not (delivered / "2022.1.00055.S").exists()
    assert manifest.get("science_goal_uid") == "uid://A001/X2d20/X3ca0"


def test_recursive_unpack_defaults_unpack_aux_and_caltables_but_not_flagversions(tmp_path: Path) -> None:
    delivered = tmp_path / "member.uid___X" / "delivered"
    delivered.mkdir(parents=True)
    top = delivered / "outer_aux.tar"

    nested_aux = _tar_bytes({"qa/pipeline_aquareport.xml": b"<Root/>"})
    nested_caltables = _tar_bytes({"calibration/table.txt": b"tab\n"})
    nested_flagversions = _tar_bytes({"calibration/flags.txt": b"flags\n"})

    base = (
        "2022.1.00055.S/science_goal.uid___A001_X2d20_X3ca0/"
        "group.uid___A001_X2d20_X3ca1/member.uid___A001_X2d20_X3ca2/calibration"
    )
    _write_tar(
        top,
        {
            f"{base}/member.uid___A001_X2d20_X3ca2.hifa_calimage.auxproducts.tgz": nested_aux,
            f"{base}/member.uid___A001_X2d20_X3ca2.session_1.caltables.tgz": nested_caltables,
            f"{base}/uid___A002_X10065c7_X435b.ms.flagversions.tgz": nested_flagversions,
        },
    )

    manifest_path = delivered.parent / "almaBulkManifest.json"
    atomic_write_json(
        manifest_path,
        {
            "mous_uid": "uid://A001/X2d20/X3ca2",
            "project_code": "2022.1.00055.S",
            "artifacts": [
                {
                    "kind": "auxiliary",
                    "filename": top.name,
                    "local_path": str(top),
                    "status": "present",
                }
            ],
        },
    )

    unpack_mous_delivered(delivered, manifest_path)
    assert (delivered / "calibration" / "table.txt").exists()
    assert (delivered / "calibration" / "uid___A002_X10065c7_X435b.ms.flagversions.tgz").exists()
    assert (delivered / "calibration" / "flags.txt").exists() is False
    assert (delivered / "calibration" / "member.uid___A001_X2d20_X3ca2.session_1.caltables.tgz").exists() is False
    assert (delivered / "calibration" / "member.uid___A001_X2d20_X3ca2.hifa_calimage.auxproducts.tgz").exists() is False
