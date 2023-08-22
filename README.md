# persidict

Simple persistent dictionaries for Python.

## What Is It?

`persidict` offers a simple persistent key-value store for Python. 
It saves the content of the dictionary in a folder on a disk 
or in an S3 bucket on AWS. Each value is stored as a separate file.
Only text strings, or sequences of strings, are allowed as keys.

## How To Get It?

The source code is hosted on GitHub at:
[https://github.com/vladlpavlov/persidict](https://github.com/vladlpavlov/persidict) 

Binary installers for the latest released version are available at the Python package index at:
[https://pypi.org/project/persidict](https://pypi.org/project/persidict)

        pip install persidict

## Dependencies

* [pandas](https://pandas.pydata.org)
* [scikit-learn](https://scikit-learn.org) 
* [numpy](https://numpy.org)
* [boto3](https://boto3.readthedocs.io)
* [moto](http://getmoto.org)
* [jsonpickle](https://jsonpickle.github.io)
* [pytest](https://pytest.org)

## Key Contacts

* [Vlad (Volodymyr) Pavlov](https://www.linkedin.com/in/vlpavlov/) - Initial work