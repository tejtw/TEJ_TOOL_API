# coding=UTF-8
import sys
from pathlib import Path
from setuptools import Extension, setup  # noqa: E402


  


install_requires = [
    'dask >= 2022.4.1',
    'pandas >= 1.2.5, <= 1.5.3',
    'tejapi >= 0.1.27',
    'xlrd >= 1.0.0',
    'openpyxl',
    'fastparquet <=0.8.3',
]

installs_for_two = [
    'pyOpenSSL',
    'ndg-httpsclient',
    'pyasn1'
]

if sys.version_info[0] < 3:
    install_requires += installs_for_two

packages = [
    'TejToolAPI',
    'TejToolAPI.tables',
]

setup_requirements = [
    'setuptools_scm>=3.5.0',
]
def myversion():
    def my_release_branch_semver_version(version):
        return str(version.tag)

    return {
        'version_scheme': my_release_branch_semver_version,
        'local_scheme': 'no-local-version',
    }
this_directionary = Path(__file__).parent
long_description = (this_directionary/"README.md").read_text(encoding='utf-8')
setup(
    name='tej-tool-api',
    description='Package to fetch a large quantity of data from tejapi.',
    keywords=['tej', 'big data', 'data', 'financial', 'economic','stock','TEJ',],
    long_description=long_description,
    long_description_content_type='text/markdown',
    #version='1.0.16',
    use_scm_version = myversion,
    author='tej',
    author_email='tej@tej.com.tw',
    maintainer='tej api Development Team',
    maintainer_email='tej@tej.com',
    url='https://api.tej.com.tw',
    license='MIT',
    install_requires=install_requires,
    tests_require=[
        'unittest2',
        'flake8',
        'nose',
        'httpretty',
        'mock',
        'factory_boy',
        'jsondate'
    ],
    test_suite="nose.collector",
    
    packages=packages,
    package_data = {'': ['*.csv','*.xlsx','*.json']},
    include_package_data=True,
    
    python_requires=">=3.8"
)