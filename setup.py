from setuptools import setup

setup(
    name="hardlink-dedup",
    packages=["hardlink_dedup"],
    entry_points={
        "console_scripts": [
            "hardlink-dedup=hardlink_dedup.app:main",
        ]
    },
)
