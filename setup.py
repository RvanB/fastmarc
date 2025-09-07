# setup.py
from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        name="fastmarc.reader",        # package.module
        sources=["fastmarc/reader.pyx"],
        language="c",                  # change to "c++" if your .pyx uses C++
        # extra_compile_args=[], extra_link_args=[],
    )
]

setup(
    name="fastmarc",
    version="0.1.0",
    packages=["fastmarc"],
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": 3,
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
        },
    ),
    install_requires=["pymarc>=5.1"],
)
