from setuptools import setup, find_packages

setup(
    name="trend_analysis_dagster",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagit",
        "firebase-admin",
        "openai",
        "python-dotenv"
    ]
)