# persidict

Simple persistent dictionaries for distributed applications in Python.

## 1. What Is It?

**`persidict`** offers a simple persistent key-value store for Python. 
It saves the content of the dictionary in a folder on a disk 
or in an S3 bucket on AWS. Each value is stored as a separate file / S3 object.
Only text strings or sequences of strings are allowed as keys.

Unlike other persistent dictionaries (e.g. Python's native `shelve`), 
`persidict` is designed for use in highly **distributed environments**, 
where multiple instances of a program run concurrently across many machines,
accessing the same dictionary via a shared storage.

## 2. Usage

### 2.1 Storing Data on a Local Disk

The **`FileDirDict`** class saves your dictionary to a local folder. 
Each key-value pair is stored as a separate file.

```python
from persidict import FileDirDict

# Create a dictionary that will be stored in the "my_app_data" folder.
# The folder will be created automatically if it doesn't exist.
app_settings = FileDirDict(base_dir="my_app_data")

# Add and update items just like a regular dictionary.
app_settings["username"] = "alex"
app_settings["theme"] = "dark"
app_settings["notifications_enabled"] = True

# Values can be any pickleable Python object.
app_settings["recent_projects"] = ["project_a", "project_b"]

print(f"Current theme is: {app_settings['theme']}")
# >>> Current theme is: dark

# The data persists!
# If you run the script again or create a new dictionary object
# pointing to the same folder, the data will be there.
reloaded_settings = FileDirDict(base_dir="my_app_data")

print(f"Number of settings: {len(reloaded_settings)}")
# >>> Number of settings: 4

print("username" in reloaded_settings)
# >>> True
```
### 2.2 Storing Data in the Cloud (AWS S3)

For distributed applications, you can use **`S3Dict`** to store data in 
an AWS S3 bucket. The usage is identical, allowing you to switch 
between local and cloud storage with minimal code changes.

```python
from persidict import S3Dict

# Create a dictionary that will be stored in an S3 bucket.
# The bucket will be created if it doesn't exist.
cloud_config = S3Dict(bucket_name="my-app-config-bucket")

# Use it just like a FileDirDict.
cloud_config["api_key"] = "ABC-123-XYZ"
cloud_config["timeout_seconds"] = 30

print(f"API Key: {cloud_config['api_key']}")
# >>> API Key: ABC-123-XYZ
```


## 3. Glossary

### 3.1 Core Concepts

* **`PersiDict`**: The abstract base class that defines the common interface 
for all persistent dictionaries in the package. It's the foundation 
upon which everything else is built.
* **`PersiDictKey`**: A type hint that specifies what can be used
as a key in any `PersiDict`. It can be a `SafeStrTuple`, 
a single string, or a sequence of strings.
* **`SafeStrTuple`**: The core data structure for keys. It's an immutable, 
flat tuple of non-empty, URL/filename-safe strings, ensuring that 
keys are consistent and safe for various storage backends.

### 3.2 Main Implementations

* **`FileDirDict`**: A primary, concrete implementation of `PersiDict` 
that stores each key-value pair as a separate file in a local directory.
* **`S3Dict`**: The other primary implementation of `PersiDict`, 
which stores each key-value pair as an object in an AWS S3 bucket, 
suitable for distributed environments.

### 3.3 Key Parameters

* **`file_type`**: A key parameter for `FileDirDict` and `S3Dict` that 
determines the serialization format for values. 
Common options are `"pkl"` (pickle) and `"json"`. 
Any other value is treated as plain text for string storage.
* **`base_class_for_values`**: An optional parameter for any `PersiDict` 
that enforces type checking on all stored values, ensuring they are 
instances of a specific class.
* **`immutable_items`**: A boolean parameter that can make a `PersiDict` 
"write-once," preventing any modification or deletion of existing items.
* **`digest_len`**: An integer that specifies the length of a hash suffix 
added to key components to prevent collisions on case-insensitive file systems.

### 3.4 Advanced Classes

* **`WriteOnceDict`**: A wrapper that enforces write-once behavior 
on any `PersiDict`, ignoring subsequent writes to the same key.
* **`OverlappingMultiDict`**: An advanced container that holds 
multiple `PersiDict` instances sharing the same storage 
but with different `file_type`s.

### 3.5 Special "Joker" Values

* **`Joker`**: The base class for special command-like values that 
can be assigned to a key to trigger an action instead of storing a value.
* **`KEEP_CURRENT`**: A "joker" value that, when assigned to a key, 
ensures the existing value is not changed.
* **`DELETE_CURRENT`**: A "joker" value that deletes the key-value pair 
from the dictionary when assigned to a key.

## 4. Comparison With Python Built-in Dictionaries

### 4.1 Similarities 

`PersiDict` and its subclasses can be used as regular Python dictionaries. 

* You can use square brackets to get, set, or delete values. 
* You can iterate over keys, values, or items. 
* You can check if a key is in the dictionary. 
* You can check whether two dicts are equal
(meaning they contain the same key-value pairs).
* You can get the length of the dictionary.
* Methods `keys()`, `values()`, `items()`, `get()`, `clear()`
, `setdefault()`, `update()` etc. work as expected.

### 4.2 Differences 

**`PersiDict`** and its subclasses persist values between program executions, 
as well as make it possible to concurrently run programs 
that simultaneously work with the same instance of a dictionary.

* Keys must be sequences of URL/filename-safe non-empty strings.
* Values must be pickleable Python objects.
* You can constrain values to be an instance of a specific class.
* Insertion order is not preserved.
* You cannot assign initial key-value pairs to a dictionary in its constructor.
* **`PersiDict`** API has additional methods `delete_if_exists()`, `timestamp()`,
`get_subdict()`, `subdicts()`, `random_key()`, `newest_keys()`, 
`oldest_keys()`, `newest_values()`, `oldest_values()`, and
`get_params()`, which are not available in native Python dicts.
* You can use `KEEP_CURRENT` constant as a fake new value 
to avoid actually setting/updating a value. Or `DELETE_CURRENT` as 
a fake new value to delete the previous value from a dictionary.

## 5. How To Get It?

The source code is hosted on GitHub at:
[https://github.com/pythagoras-dev/persidict](https://github.com/pythagoras-dev/persidict) 

Binary installers for the latest released version are available at the Python package index at:
[https://pypi.org/project/persidict](https://pypi.org/project/persidict)

### 5.1 Using uv :
```
uv add persidict
```

### 5.2 Using pip (legacy alternative to uv):
```
pip install persidict
```

## 6. Dependencies

* [jsonpickle](https://jsonpickle.github.io)
* [joblib](https://joblib.readthedocs.io)
* [lz4](https://python-lz4.readthedocs.io)
* [pandas](https://pandas.pydata.org)
* [numpy](https://numpy.org)
* [boto3](https://boto3.readthedocs.io)
* [pytest](https://pytest.org)
* [moto](http://getmoto.org)

## 7. Key Contacts

* [Vlad (Volodymyr) Pavlov](https://www.linkedin.com/in/vlpavlov/)