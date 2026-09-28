import subprocess
from functools import lru_cache


@lru_cache(maxsize=1)
def get_git_commit() -> str:
    """
    Short git commit hash of the recorder at the time it was started, so
    every parquet file can be traced back to the code version that produced
    it. Falls back to "unknown" outside a git checkout (e.g. a packaged
    deploy) instead of failing.
    """

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"
