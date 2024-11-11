from setuptools import setup, find_packages

setup(
    name="beyond_bets",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.0.0",
    ],
)
