from setuptools import find_packages, setup
setup(
    name='cassandralvea',
    packages=find_packages(include=['cassandralvea']),
    version='0.1.0',
    description='Cassandra Python library',
    author='Lenin Escobar',
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite='cassandralveatest',
)