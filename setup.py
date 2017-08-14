from distutils.core import setup

setup(
    name='gapi',
    version='1.1.0',
    author='Scott Hendrickson, Josh Montague, Jeff Kolb, Fiona Pigott',
    author_email='des-services@twitter.com',
    packages=['search'],
    scripts=['gnip_search.py', 'gnip_time_series.py'],
    url='https://github.com/fionapigott/Gnip-Python-Search-API-Utilities',
    download_url='https://github.com/fionapigott/Gnip-Python-Search-API-Utilities/tags/',
    license='LICENSE.txt',
    description='Simple utilties to explore the Gnip search API',
    install_requires=[
         "tweet_parser >=1.0.5",
         "sngrams >= 0.2.0" ,
         "requests > 2.4.0"
        ],
    extras_require = {
        'timeseries':  ["numpy >= 1.10.1"
                , "scipy >= 0.16.1"
                , "statsmodels >= 0.6.1"
                , "matplotlib >= 1.5.0"
                , "pandas >= 0.17.0"
                ],
        }
    )
