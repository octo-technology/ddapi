# Developer Guidelines

Hi there ! In this document, we'll cover all the different guidelines, 
recommandations, and best practices we want you to keep in mind while 
contributing to the ddapi project. This document must be kept up-to-date with
the current team views and opinions, and not taken as a frozen set of 
immutable rules.

## Overview
* Build and install from local, and test
* Organisation & Packaging
* Release Process
* Absolute vs Relative imports
* Docstrings
* Methods ordering

## Build and install from local

We use [conda](https://conda.io/docs/) to work on ddapi and manage virtual environments.

#### virtualenv

    make develop_env

It creates a new conda environment and adds 
requirements listed in the requirements.txt file plus dev_requirements.txt.
    
#### install from local folder

The best way to install ddapi from a local git repository is to use setuptools' develop installation
mode. There is a shortcut for that in the Makefile :
    
    make develop

It creates a symlink in the site-packages folder of the python associated wtih the virtualenv.

#### run tests

Run tests and show actual coverage. The Jenkinsfile is used by Jenkins (Build Factory) and defines the coverage value
you can't be below

    make test
    
    docker run -p 9000:9000 -e "MINIO_ACCESS_KEY=key_test" -e "MINIO_SECRET_KEY=secret_test" minio/minio server /data
    make integration_test
    
    make coverage

#### compile 
 
The following commands will pull dependencies from the meta.yaml file, and then build the sources.

    make package
    
It creates 2 archive files in the current folder

ex :

    ./dist/pyddapi-X.X.X.dev0.tar.tgz
    ./dist/pyddapi-X.X.X.dev0-py2-none-any.whl


## Organisation & Packaging

We follow these principles : [python packaging](https://blog.ionelmc.ro/presentations/packaging/#slide:1)

Very Important Files : 

 * **setup.py** file contains the current version of the package.
 We use [zest-releazer](http://zestreleaser.readthedocs.io/en/latest/overview.html) 
 to release and to version every artifacts of DD's project 
 * **MANIFEST.in** file lists the 'non python files' to be included 
 in the archive package by setuptools
 * **Makefile** file contains targets about the life cycle of the project 
 (test, install, package, develop, etc..)
 * **ci/Jenkinsfile** file defines the pipeline used by Jenkins to build, to test, and to push artifacts
    * each git branch has its own build (see [Jenkins' Multibranch pipeline feature](https://jenkins.io/doc/tutorials/build-a-multibranch-pipeline-project/))
    * each build pushes the final artifact to [anaconda.org](https://anaconda.org/octo/dashboard) 
    with **the label set to the branch's name** (see [Anaconda's label feature](https://docs.anaconda.com/anaconda-cloud/user-guide/tutorials))
 * **setup.cfg** file lists all packages to install with pyddapi to run correctly
    * if you want to add a new package, please verify if there is a binary version on PYPI
    (bdist_wheel / any_wheel). If no, add the conda package installation instead in the Makfile
    into the **env target** and/or in the **develop target**
    * ***you should never add GCC or any compilation dependency*** to facilitate installation 
 * **ci/test_requirements.txt** file lists only packages required for development purpose (testing, coverage, linter, etc...)
 * **ci/Dockerfile** file is used by Jenkins to build ddapi inside a docker container for isolation and idempotency
 * **CHANGELOG.md** file is completed by zest-releaser with all commit messages starting 
 after the previous release to the next one

## Release process

 * install [zest-releaser](https://zestreleaser.readthedocs.io/en/latest/)
 * update the [changelog](./CHANGELOG.md) and commit it
 * use the command ```$> fullrelease``` in the terminal
 * the pipeline Jenkins detects the new tag (ex: 3.0.0), builds the artefact and pushes it
 to [anaconda.org](https://anaconda.org/octo) and [pypi.org](https://pypi.org/)
 

## Imports
[PEP0328](https://www.python.org/dev/peps/pep-0328/) defines the concept of
relative imports. The rationale behind it is to remove the ambiguity between a
local package and a system level package. For example, if your current
directory looks like this :

    my_package/
        animals/
            __init__.py
            pandas.py
            giraffes.py
        things/
            __init__.py
            chair.py

and if the content of your animals/__init__.py file is as follows:

```python
from pandas import cuddles
```

then the python interpreter (and the reader of your code) can't know if you
meant that you want to import the content of the pandas.py file, or if you
wanted to import the pandas library that is installed somewhere in your
PYTHONPATH.

In this case, it is highly recommended you use the relative import notation,
which allows for the disambiguity of the import line:

```python
from .pandas import cuddles
```

However, in most cases, it is even better to give the full import path, in
order to completely explicit from which package you are importing new sources.
The better init file would then look like this:

```python
from my_package.animals.pandas import cuddles
```

In this project, we encourage the use of relative imports for same-level
imports, but disapprove the use of multiple levels relative imports. Some
examples:

```python
from .pandas import cuddles  # GOOD
from my_package.animals.pandas import cuddles  # GOOD
import giraffes  # BAD
import ..animals  # BAD
```

When importing multiple objects from a single module, you might run out of
space to write all your imports on a single line. For example :

```python
from my_package.animals import pandas, giraffes, goats, horses, dogs, chi..
```

In those cases, you are faced with two options:

```python
from my_package.animals import pandas, giraffes, goats, horses, dogs, \
    chickens, parrots
```

or

```python
from my_package.animals import (pandas, giraffes, goats, horses, dogs, 
    chickens, parrots)
```

The PyCharm IDE favors the former by default, so we will stick to this notation
for now.

## Docstrings
[PEP0257](https://www.python.org/dev/peps/pep-0257/) describes what a docstring
is a what should contain, but does not explicitly states how the content should
be formatted. Some conventions have emerged, such as the Numpy Style and the
Google Style. We will use the later in our code. 

You may find a document that closely follows the google style [here](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)

## Methods Ordering
In order to keep methods ordering consistent across the codebase, allowing for 
easier navigation, we settled for the following set of rules:
* \_\_init\_\_ method first, then
* magic methods (\_\_getattr\_\_, \_\_str\_\_, ...), sorted alphanumerically, then
* properties, and associated setter, sorted alphanumerically, then
* normal methods, sorted alphanumerically, then
* "private" methods (meaning methods starting with an underscore)

Your class should thus look like this :
```python
class MyClass(object):
    def __init__(self):
        self._attribute = 0
        
    def __str__(self):
        return "MyClass"
    
    @property
    def my_attribute(self):
        return self._attribute
        
    @my_attribute.setter
    def my_attribute(self, value):
        self._attribute = value
        
    def first_method(self):
        pass
        
    def second_method(self, param):
        pass
        
    def _my_private_method(self):
        pass
```

All classes in the codebase might not enforce this set of rules. If you happen 
to stumble upon one of these classes, please apply the [boy scout rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule).
