from pathlib import Path

from core_v02.test_cleanup import TestArtifactsCleaner


_cleaner = TestArtifactsCleaner(Path(__file__).resolve().parent)


def pytest_sessionstart(session):  # noqa: ANN001
    _cleaner.snapshot()


def pytest_sessionfinish(session, exitstatus):  # noqa: ANN001
    _cleaner.cleanup_new_artifacts()

