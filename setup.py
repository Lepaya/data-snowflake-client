import codecs
import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = "1.1"
DESCRIPTION = "Python snowflake client for Lepaya"
LONG_DESCRIPTION = (
    "A package that allows to interact with the snowflake api and load/extract data from Snowflake"
)

# Setting up
setup(
    name="lepaya_python_snowflakeclient",
    version=VERSION,
    author="Humaid Mollah",
    author_email="humaid.mollah@lepaya.com",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=[
        'pandas~=1.4',
        'pyarrow==8.0.0',
        'snowflake-connector-python==2.7.11',
        'structlog~=22.3',
        'pydantic~=1.9',
        'git+https://github.com/Lepaya/lepaya-python-slackclient.git@release-1.2'
    ],
    keywords=["python", "snowflake", "dataframe", "extract", "load", "table"],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Lepaya Python Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ],
)
