# -*- Import: -*-
from paver.easy import *
from paver import setuputils
#from distutils.core import setup
from setuptools import find_packages
from paver.setuputils import setup
#see http://docs.python.org/distutils/extending.html#integrating-new-commands
from distutils.command.build_py import build_py as _build_py
from distutils import log

PROJECT = 'mongosearch'

try:
    # Optional tasks, only needed for development
    # -*- Optional import: -*-
    from github.tools.task import *
    import paver.doctools
    import paver.virtual
    import paver.misctasks
    ALL_TASKS_LOADED = True
except ImportError, e:
    info("some tasks could not not be imported.")
    debug(str(e))
    ALL_TASKS_LOADED = False

setuputils.standard_exclude+=('.gitignore',)
setuputils.standard_exclude_directories+=('.git',)


PACKAGE_DATA = setuputils.find_package_data(PROJECT, 
                                            package=PROJECT,
                                            only_in_packages=False,)

version = '0.1-alpha'

classifiers = [
    # Get more strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: BSD License",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Programming Language :: JavaScript",
    "Topic :: Database",
    "Topic :: Text Processing :: Indexing"
    "Natural Language :: English",
    ]

install_requires = [
    # -*- Install requires: -*-
    'setuptools',
    'pymongo >= 1.6'
    ]

build_requires = [
    # -*- Install requires: -*-
    'setuptools',
    'GitPython >= 0.2.0-beta1'
    ]

entry_points="""
    # -*- Entry points: -*-
    """


class build_py(_build_py, object):
    """
    git-submodule-happy Python source builder.
    
    We inherit from object to make sur the `super` call works
    """
    
    git_submodules = ['mongosearch/javascript']
    
    def run(self, *args, **kwargs):
        """
        we hack the normal build to force the git submodule to be updated.
        """
        self.ensure_submodules()
        super(build_py, self).run(*args, **kwargs)
        
    def ensure_submodules(self, *args, **kwargs):
        """
        given the defined list of submodules in `self.gitsubmodules`,
        ensure they are all checked out. (simplistic style - simply checks for
        any non-hidden files using `glob`.)
        """
        import os
        from glob import glob
        from distutils.util import convert_path
        missing_submodule = False
        for submodule_path in self.git_submodules:
            file_list = glob(convert_path(
              os.path.join(submodule_path, '*')
            ))
            if len(file_list) : continue
            log.debug('missing submodule %s' % submodule_path)
            missing_submodule = True
        if missing_submodule:
            self.update_submodules()

    def update_submodules(self, *args, **kwargs):
        """
        actually do submodule updating.
        separate functnion call so we can isolate teh GitPython import
        
        Tested with git.__version__=='0.2.0-beta1'
        """
        import git
        
        repo = git.Git('.')
        repo.submodule('init')
        repo.submodule('update')

setup(
    cmdclass={'build_py': build_py},
    name=PROJECT,
    version=version,
    description='Full text search for mongo in javascript, with python client driver',
    long_description=open('README.rst', 'r').read(),
    classifiers=classifiers,
    keywords='fulltextsearch search mongodb javascript',
    author='Andy and Dan MacKinlay',
    author_email='fillmewithspam@email.possumpalace.org',
    url='http://www.assembla.com/spaces/python-mongo-search',
    license='BSD',
    packages = find_packages(exclude=['bootstrap', 'pavement',]),
    package_dir = {'mongosearch': 'mongosearch'},
    include_package_data=True,
    package_data=PACKAGE_DATA,
    test_suite='nose.collector',
    zip_safe=False,
    install_requires=install_requires,
    entry_points=entry_points,
)

options(
    # -*- Paver options: -*-
    minilib=Bunch(
        extra_files=[
            # -*- Minilib extra files: -*-
            ]
        ),
    sphinx=Bunch(
        docroot='docs',
        builddir="_build",
        sourcedir=""
        ),
    virtualenv=Bunch(
        packages_to_install=[
            # -*- Virtualenv packages to install: -*-
            'github-tools',
            "nose",
            "Sphinx>=0.6b1",
            "pkginfo", 
            "virtualenv"],
        dest_dir='./virtual-env/',
        install_paver=True,
        script_name='bootstrap.py',
        paver_command_line=None
        ),
    )


if ALL_TASKS_LOADED:
    @task
    @needs('generate_setup', 'minilib', 'setuptools.command.sdist')
    def sdist():
        """Overrides sdist to make sure that our setup.py is generated."""
