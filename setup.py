from distutils.core import setup

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
    version="%d.%d.%d" % __import__('minitds').VERSION,
    url='https://github.com/nakagami/minitds/',
    classifiers=classifiers,
    keywords=['SQLServer'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='Yet another SQLServer database driver',
    long_description=open('README.rst').read(),
    license="MIT",
    py_modules=['minitds'],
)
