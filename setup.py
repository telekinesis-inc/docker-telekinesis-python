import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="telekinesis_compute",
    version="0.0.1",
    author="Telekinesis, Inc.",
    author_email="support@telekinesis.cloud",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/telekinesis-cloud/telekinesis-compute",
    packages=setuptools.find_packages(),
    package_data={'telekinesis_compute': ['Dockerfile_python', 'Dockerfile_js']},
    install_requires=["docker", "telekinesis"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
