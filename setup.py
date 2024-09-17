import codecs
import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

DESCRIPTION = "Python snowflake client for Lepaya Data team"
LONG_DESCRIPTION = (
    "A python package that allows to interact with the snowflake api and load/extract data from Snowflake"
)

# Setting up
setup(
    name="data_snowflake_client",
    version="6.0.0",
    author="Humaid Mollah",
    author_email="humaid.mollah@lepaya.com",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=[
        "data-slack-client @ git+https://github.com/Lepaya/data-slack-client",
        "pandas~=2.2",
        "pyarrow~=17.0",
        "snowflake-connector-python~=3.5",
        "structlog~=22.3",
        "pydantic~=1.9",
        "cryptography~=42.0",
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
