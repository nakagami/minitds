import sys
from distutils.core import setup, Command


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from minitds import test_minitds
        import unittest
        unittest.main(test_minitds, argv=sys.argv[:1])

cmdclass = {'test': TestCommand}

version = "%d.%d.%d" % __import__('minitds').VERSION

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name="minitds",
    version=version,
    url='https://github.com/nakagami/minitds/',
    classifiers=classifiers,
    keywords=['SQLServer'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='Yet another SQLServer database driver',
    license="MIT",
    packages=['minitds'],
    cmdclass=cmdclass,
)
