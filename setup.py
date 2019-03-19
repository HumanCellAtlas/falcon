from setuptools import setup

setup(
    name='falcon',
    packages=['falcon'],
    include_package_data=True,
    install_requires=[
        'cromwell-tools==1.1.1',
        'flask==1.0.2',
        'pytest==3.6.3',
        'pytest-timeout==1.3.1',
        'pytest-cov==2.5.1',
    ],
)
