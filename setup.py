from setuptools import setup, find_packages


setup(
    name="pseudonym",
    version="0.5.10",
    author="Jonathan Klaassen",
    author_email="jonathan@livefyre.com",
    description=("A library for configuring elasticsearch aliases."),
    url="https://github.com/Livefyre/pseudonym",
    packages=find_packages(exclude=['tests']),
    long_description="",
    entry_points={
        'console_scripts': ['pseudonym=pseudonym.cli:main']
    },
    install_requires=['docopt'],
    test_suite='nose.collector'
)
