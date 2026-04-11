from setuptools import setup, find_packages
from pathlib import Path

requirements_path = Path(__file__).parent / "requirements.txt"
install_requires = []
if requirements_path.exists():
    with open(requirements_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                install_requires.append(line)

setup(
    name="PD_TEST_Langtaosha",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=install_requires,
    python_requires=">=3.8",
)
