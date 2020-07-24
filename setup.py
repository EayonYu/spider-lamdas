import os

from setuptools import setup, find_packages


# get requirements from requirements.txt
install_requires = list()
with open("requirements.txt", "r") as f:
    for line in f:
        required_package = line.rstrip()
        if required_package:
            install_requires.append(required_package)

# get tag from env
if os.environ.get('CI_COMMIT_TAG'):
    version = os.environ['CI_COMMIT_TAG']
elif os.environ.get('VERSION'):
    version = os.environ['VERSION']
else:
    version = None

if version:
    setup(
        name='spider_lambdas',
        version=version,
        description='lambdas for spider',
        long_description='lambdas for spider',
        long_description_content_type='text/plain',
        author='jacob.hong',
        author_email='jacob.hong@tcl.com',
        license='MIT',
        packages=find_packages(),
        url='http://10.124.106.120:18080/cloudplatform/gaia/spider/lambdas',
        zip_safe=False,
        install_requires=install_requires,
        dependency_links=[
            "http://52.81.42.99:31187/simple"
        ],
        python_requires='>=3.7, <=3.8',
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Topic :: Software Development :: spider lambdas',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3.7',
        ],
        project_urls={
            'Source': 'http://10.124.106.120:18080/cloudplatform/gaia/spider/lambdas',
        },
    )
