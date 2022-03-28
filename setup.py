#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst", encoding="utf-8") as history_file:
    history = history_file.read()

requirements = [
    "Click>=7.1.2",
    "asciichartpy",
    "tabulate",
]

test_requirements = [
    "pytest>=3",
    "numpy",
]

setup(
    author="Daniel C. Moura",
    author_email="daniel.c.moura@gmail.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Text Processing",
        "Topic :: Utilities",
    ],
    description="SpyQL: SQL with Python in the midle",
    entry_points={
        "console_scripts": [
            "spyql=spyql.cli:main",
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="sql data csv json",
    name="spyql",
    packages=find_packages(include=["spyql", "spyql.*"]),
    test_suite="tests",
    tests_require=test_requirements + requirements,
    url="https://github.com/dcmoura/spyql",
    project_urls={
        "Documentation": "https://spyql.readthedocs.io",
        "Source": "https://github.com/dcmoura/spyql",
    },
    version="0.5.0",
    zip_safe=False,
)
