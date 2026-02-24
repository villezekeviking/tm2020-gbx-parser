from setuptools import setup, find_packages

setup(
    name="tm2020-gbx-parser",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        # No required dependencies - pure Python stdlib
    ],
    extras_require={
        'lzo': ['python-lzo>=1.14'],  # Optional for body decompression
        'dev': ['pytest>=7.0'],
    },
    python_requires='>=3.7',
    author="villezekeviking",
    description="Pure-Python parser for TrackMania 2020 GBX replay files",
    url="https://github.com/villezekeviking/tm2020-gbx-parser",
)
