from setuptools import setup
import os

VERSION = "v0.6.4"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="PgQueuer",
    description="PgQueuer is now pgqueuer",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    version=VERSION,
    install_requires=["pgqueuer"],
    classifiers=["Development Status :: 7 - Inactive"],
)
