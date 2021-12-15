from setuptools import find_packages, setup

NAME = "feast_lmdb"
REQUIRES_PYTHON = ">=3.7.0"

setup(
    name=NAME,
    description=open("README.md").read(),
    version="0.0.1",
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(include=["feast_lmdb"]),
    install_requires=[
        "lmdb==1.2.*",
        "feast==0.16.*"
    ],
    license="Apache",
)
