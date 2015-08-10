from setuptools import setup, find_packages


setup(
    name="pseudonym",
    version="0.0.4",
    author="Jonathan Klaassen",
    author_email="jonathan@livefyre.com",
    description=("A library for configuring elasticsearch aliases."),
    url="https://github.com/Livefyre/pseudonym",
    packages=find_packages(exclude=['tests']),
    long_description="",
    test_suite='nose.collector'
)
