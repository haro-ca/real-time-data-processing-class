"""Create the separate Python 3.11 virtual environment used by PyFlink.

PyFlink 1.19 does not support Python 3.14, and it pins a different py4j version
than Spark 4, so it lives in its own venv. The rest of the project (Spark,
producer, analyzer) uses the default `.venv` on Python 3.14.

Usage:
    uv run python src/setup_flink_venv.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
VENV = ROOT / ".venv-flink"
PYTHON = VENV / "bin" / "python"


def main():
    print(f"Creating PyFlink virtual environment at {VENV} ...")
    subprocess.run(
        ["uv", "venv", "--python", "3.11", str(VENV)],
        check=True,
    )

    print("Installing build tooling for PyFlink dependencies ...")
    # apache-beam's source build needs an older setuptools that still ships
    # pkg_resources, plus wheel. Install them into the venv and build without
    # isolation so the build backend can find them.
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(PYTHON),
            "setuptools==69.5.1",
            "wheel",
        ],
        check=True,
    )

    print("Installing apache-flink==1.19.1 into PyFlink venv ...")
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(PYTHON),
            "--no-build-isolation",
            "apache-flink==1.19.1",
        ],
        check=True,
    )

    print(f"\nPyFlink venv ready. Use {PYTHON} to run the Flink pipeline.")


if __name__ == "__main__":
    main()
