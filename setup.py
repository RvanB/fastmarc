# setup.py
from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        name="fastmarc.reader",
        sources=["fastmarc/reader.pyx"],
        language="c",
        extra_compile_args=["-O3"],  # Optimize for speed
    ),
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
            "initializedcheck": False,
        },
    ),
    install_requires=["pymarc>=5.1"],
)
