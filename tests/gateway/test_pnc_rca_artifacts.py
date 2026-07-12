from pathlib import Path

from gateway.pnc_rca_artifacts import local_candidates_for_vm_path, write_vm_tmp_text


def test_local_candidates_for_vm_tmp_path():
    home = Path('/Users/test')

    candidates = local_candidates_for_vm_path('/mnt/tmp/g1q3_task/rca_execution_request.json', home=home)

    assert candidates == [
        home / 'Mounts' / 'mini_root' / 'mnt' / 'tmp' / 'g1q3_task' / 'rca_execution_request.json',
        home / 'Mounts' / 'department-pnc_team-planning_algo-driving' / 'tmp' / 'g1q3_task' / 'rca_execution_request.json',
    ]


def test_local_candidates_reject_non_tmp_or_traversal():
    assert local_candidates_for_vm_path('/home/mini/file.json', home=Path('/Users/test')) == []
    assert local_candidates_for_vm_path('/mnt/tmp/../bad.json', home=Path('/Users/test')) == []


def test_write_vm_tmp_text_uses_first_writable_mount(tmp_path):
    home = tmp_path / 'home'
    target = '/mnt/tmp/g1q3_task/rca_execution_request.json'

    written = write_vm_tmp_text(target, '{"ok":true}', home=home)

    assert written == home / 'Mounts' / 'mini_root' / 'mnt' / 'tmp' / 'g1q3_task' / 'rca_execution_request.json'
    assert written.read_text(encoding='utf-8') == '{"ok":true}'
