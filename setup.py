from setuptools import setup, find_packages

setup(
    name="covidactnow.datapublic",
    version="0.1.0",
    packages=["covidactnow.datapublic"],
    url="https://github.com/covid-projections/covid-data-public",
    author="covidactnow.com",
    # List Python distribution packages that need to be installed when setting up
    # the `packages` (above) in `install_requires` (below).
    # Somewhat confusingly there is other code in this repo that is not installed by
    # setuptools. The dependencies of that code are listed in requirements.txt.
    install_requires=["pandas", "structlog", "structlog-sentry"],
)
