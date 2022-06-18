from setuptools import setup, find_packages

setup(
    name='first_pipeline',
    version='1.0',
    packages=find_packages(include=('job*',)),
    description='first_pipeline',
    install_requires=[
        'prophecy-libs==1.1.3'
    ],
    entry_points={
        'console_scripts': [
            'main = job.pipeline:main',
        ],
    },
    extras_require={
        'test': ['pytest', 'pytest-html'],
    }
)
