from setuptools import setup, find_packages

setup(
    name="tm2020-gbx-parser",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-lzo>=1.14",
    ],
    extras_require={
        'dev': ['pytest'],
    },
    python_requires='>=3.7',
    author="villezekeviking",
    description="Python parser for TrackMania 2020 GBX replay files",
    url="https://github.com/villezekeviking/tm2020-gbx-parser",
)
