import re
import ast
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('pyetl/__init__.py', 'rb') as f:
    rs = _version_re.search(f.read().decode('utf-8')).group(1)
    version = str(ast.literal_eval(rs))

setup(
    name='pyetl',
    version=version,
    install_requires=[
        'pandas>=0.19.0',
        'pydbclib>=1.2.1',
        'python-dateutil>=2.5.0'],
    description='python etl frame for small dataset',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    author='lyt',
    url='https://github.com/taogeYT/pyetl',
    author_email='liyt@vastio.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': ['pyetl = pyetl.cli:main']
    }
)
