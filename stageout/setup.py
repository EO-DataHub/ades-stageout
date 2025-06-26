from setuptools import find_packages, setup

setup(
    name="stageout",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "kubernetes",
        "pulsar-client",
        "pystac",
        "boto3",
        "botocore",
        "urllib3",
        "html-sanitizer",
    ],
    entry_points={
        "console_scripts": [
            "stageout=stageout.__main__:main",
        ],
    },
)
