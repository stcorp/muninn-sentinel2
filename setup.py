from setuptools import setup

setup(
    name="muninn-sentinel2",
    version="1.0",
    description="Muninn extension for Sentinel-2 products",
    url="https://github.com/stcorp/muninn-sentinel2",
    author="S[&]T",
    license="BSD",
    py_modules=["muninn_sentinel2"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
        "Environment :: Plugins",
    ],
    install_requires=["muninn"],
)
