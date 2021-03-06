import os

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements-dev.txt", "r") as fh:
    tests_require = [line for line in fh.read().split(os.linesep) if line]

with open("requirements.txt", "r") as fh:
    install_requires = [line for line in fh.read().split(os.linesep) if line]

setuptools.setup(
    name="skippy-kubernetes",
    version="0.0.1.dev1",
    author="Alexander Rashed",
    author_email="alexander.rashed@gmail.com",
    description="Kubernetes binding for Skippy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.dsg.tuwien.ac.at/tau/skippy-kubernetes",
    download_url="https://git.dsg.tuwien.ac.at/tau/skippy-kubernetes",
    packages=setuptools.find_packages(),
    setup_requires=['wheel'],
    test_suite="tests",
    tests_require=tests_require,
    install_requires=install_requires,
    pyton_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
    ],
)
