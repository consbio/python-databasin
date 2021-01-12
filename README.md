# python-databasin 0.5.3

[![Build Status](https://travis-ci.org/consbio/python-databasin.png?branch=master)](https://travis-ci.org/consbio/python-databasin) [![Coverage Status](https://coveralls.io/repos/consbio/python-databasin/badge.svg?branch=master&service=github)](https://coveralls.io/github/consbio/python-databasin?branch=master)

```python-databasin``` is a client library for interacting with [Data Basin](http://databasin.org).

# Install
```bash
$ pip install python-databasin
```

# Example usage
The code snippet below will import a NetCDF dataset into Data Basin and make it public. Note that the account you use 
must have import permission and the import must have complete metadata and style information.

```python
from databasin.client import Client

c = Client()
c.login('user', 'pass')

# Package must have complete metadata and style necessary for one-step import
dataset = c.import_netcdf_dataset('/path/to/netcdf_with_metadata_and_style.zip')

# One-step imports are private by default
dataset.make_public()

print(dataset.id)
print(dataset.title)
```

You can also upload Esri layer packages (`.lpk`). As with NetCDF's, layer packages for now must have the all metadata
required by Data Basin to successfully import:

```python
dataset = c.import_lpk('/path/to/lpk_with_metadata.lpk')
print(dataset.id)
print(dataset.title)
```

`python-databasin` can be used with an API key instead of a password:

```python
from databasin.client import Client

c = Client()
c.set_api_key('<username>', '<api key>')

my_datasets = c.list_my_datasets()
```
