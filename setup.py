# Automatically created by: scrapyd-deploy

from setuptools import setup, find_packages

setup(
    name='project',
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'google_maps': ['input/*.csv', ]
    },
    entry_points={'scrapy': ['settings = google_maps.settings']}
)
