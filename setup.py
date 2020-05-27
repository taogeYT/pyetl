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
    install_requires=['pydbclib>=2.1.7', 'pandas>=0.22'],
    description='Python ETL Frame',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    author='liyatao',
    url='https://github.com/taogeYT/pyetl',
    author_email='li_yatao@outlook.com',
    license='Apache 2.0',
    packages=find_packages(),
    include_package_data=False,
    zip_safe=True,
    python_requires='>=3.6',
    # entry_points={
    #     'console_scripts': ['pyetl = pyetl.cli:main']
    # }
)
