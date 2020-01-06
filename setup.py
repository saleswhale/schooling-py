import setuptools

with open('README.md', 'r') as readme:
    long_description = readme.read()

with open('requirements.txt', 'r') as reqs:
    requirements = [req.strip() for req in reqs.readlines()]

setuptools.setup(
    name='schooling',
    version='0.1',
    author='Saleswhale',
    author_email='jia@saleswhale.com',
    description='A tiny Python wrapper for Redis streams.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(exclude=['tests']),
    install_requires=requirements,
    test_requires=[
        'pytest',
        'pytest-cov',
        'pylint'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    python_requires='>=3.6'
)
