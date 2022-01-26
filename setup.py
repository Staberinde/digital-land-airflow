import os
import sys

from setuptools import find_packages, setup


# Only install black on Python 3.6 or higher
maybe_black = []
if sys.version_info > (3, 6):
    maybe_black = ["black"]

setup(
    name="digital-land-airflow",
    long_description_content_type="text/markdown",
    author="MHCLG Digital Land Team",
    author_email="DigitalLand@communities.gov.uk",
    license="MIT",
    url="https://github.com/digital-land/digital-land-airflow",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "digital-land@git+https://github.com/digital-land/digital-land-python",
        "specification@git+https://github.com/digital-land/specification",
        "GitPython~=3.1.0",
        "pyhumps~=3.5.0",
        "boto3~=1.20.0",
        "cloudpathlib",
    ],
    setup_requires=["pytest-runner"],
    extras_require={
        "test": ["coverage", "flake8", "pytest", "coveralls", "twine", "requests-mock"]
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
