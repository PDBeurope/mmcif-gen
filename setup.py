from setuptools import setup, find_packages
import os

# read the contents of your README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="investigation",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "argparse",
        "gemmi",
        "requests",
        "jq",
    ],
    entry_points={
        "console_scripts": [
            "mmcif_gen=investigation:mmcif_gen_cli",
            "make_mmcif=investigation:main",
        ],
    },
    author="Syed Ahsan Tanweer",
    author_email="ahsan@ebi.ac.uk",  # Replace with your email
    description="CLI tool for creating mmCIF files from various facility data sources",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/PDBeurope/Investigations/",  # Replace with your repo URL
    project_urls={
        "Bug Tracker": "https://github.com/PDBeurope/Investigations/issues",
        "Documentation": "https://github.com/PDBeurope/Investigations/",
        "Source Code": "https://github.com/PDBeurope/Investigations/",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    python_requires=">=3.6",
    keywords="mmcif, crystallography, structural-biology, pdbe, synchrotron",
)