from setuptools import setup

setup(name='tap-redash',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Redash API',
      url='http://singer.io',
      author='domb16',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_redash'],
      long_description=open('README.md').read(),
      install_requires=[
          'singer-python>=2.1.4',
          'requests>=2.13.0',
          'pylint',
          'pandas'
      ],
      entry_points='''
          [console_scripts]
          tap-redash=tap_redash:main
      ''',
      )

