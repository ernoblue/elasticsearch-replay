import os
from setuptools import setup

README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()
with open('requirements.txt', 'r') as x:
    REQUIREMENTS = filter(None, x.read().split('\n'))

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='elasticsearch-replay',
    version='0.1',
    packages=['elasticsearch_replay'],
    include_package_data=True,
    license='BSD License',  # example license
    description='Record and replay elasticsearch communication',
    long_description=README,
    install_requires=REQUIREMENTS,
    author='Kamil Strzelecki',
    author_email='kamil.strzelecki@gmail.com',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
)
