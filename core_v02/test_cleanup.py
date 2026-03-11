import os
import shutil
import stat
import subprocess
from pathlib import Path


PROJECT_PREFIXES = (
    "Test_v02__",
    "Smoke__",
    "P4B_test__",
)


def _safe_rmtree(path: Path) -> None:
    def _onerror(func, p, exc_info):  # noqa: ANN001
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass

    try:
        shutil.rmtree(path, onerror=_onerror)
    except Exception:
        try:
            subprocess.run(
                ["cmd", "/c", "rmdir", "/s", "/q", str(path)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass


class TestArtifactsCleaner:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.projects_root = repo_root / "local_drive" / "Projects"
        self._root_dirs_before: set[str] = set()
        self._project_dirs_before: set[str] = set()

    def snapshot(self) -> None:
        self._root_dirs_before = {
            p.name for p in self.repo_root.iterdir() if p.is_dir()
        }
        if self.projects_root.exists():
            self._project_dirs_before = {
                p.name for p in self.projects_root.iterdir() if p.is_dir()
            }
        else:
            self._project_dirs_before = set()

    def cleanup_new_artifacts(self) -> None:
        # Cleanup root-level pytest temporary cache folders.
        for p in self.repo_root.iterdir():
            if not p.is_dir():
                continue
            if p.name.startswith("pytest-cache-files-"):
                _safe_rmtree(p)

        # Cleanup test-created projects.
        if not self.projects_root.exists():
            return
        for p in self.projects_root.iterdir():
            if not p.is_dir():
                continue
            if p.name.startswith(PROJECT_PREFIXES):
                _safe_rmtree(p)
