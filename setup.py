from setuptools import find_packages, setup


setup(
    name="pcca",
    version="0.1.0",
    description="Personal Content Curation Agent (local-first)",
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=[
        "aiosqlite>=0.20.0",
        "apscheduler>=3.10.4,<4",
        "httpx>=0.27,<0.29",
        "playwright>=1.44.0",
        "feedparser>=6.0.11",
        "python-telegram-bot>=21.0,<23",
        "youtube-transcript-api>=0.6.2",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-asyncio>=0.23.0",
        ]
    },
    entry_points={"console_scripts": ["pcca=pcca.cli:main"]},
)
