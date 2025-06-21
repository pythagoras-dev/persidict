# persidict

Simple persistent dictionaries for Python.

## What Is It?

`persidict` offers a simple persistent key-value store for Python. 
It saves the content of the dictionary in a folder on a disk 
or in an S3 bucket on AWS. Each value is stored as a separate file / S3 object.
Only text strings or sequences of strings are allowed as keys.

Unlike other persistent dictionaries (e.g. Python's native `shelve`), 
`persidict` is designed for use in highly **distributed environments**, 
where multiple instances of a program run concurrently across many machines.

## Usage
Class `FileDirDict` is a persistent dictionary that stores its content 
in a folder on a disk.

    from persidict import FileDirDict    
    my_dictionary = FileDirDict(base_dir="my_folder")

Once created, it can be used as a regular Python dictionary 
that stores key-value pairs. A key must be a sequence of strings, 
a value can be any (pickleable) Python object:

    my_dictionary["Eliza"] = "MIT Eliza was a mock psychotherapist."
    my_dictionary["Eliza","year"] = 1965
    my_dictionary["Eliza","authors"] = ["Joseph Weizenbaum"]
    
    my_dictionary["Shoebox"] = "IBM Shoebox performed arithmetic operations"
    my_dictionary["Shoebox"] += " on voice commands."
    my_dictionary["Shoebox", "year"] = 1961
    my_dictionary["Shoebox", "authors"] = ["W.C. Dersch", "E.A. Quade"]

    for k in my_dictionary:
        print(list(k), "==>",  my_dictionary[k])

    if not "Eliza" in my_dictionary:
        print("Something is wrong")

If you run the code above, it will produce the following output:

    >>> ['Eliza'] ==> MIT Eliza was a mock psychotherapist.
    >>> ['Shoebox'] ==> IBM Shoebox performed arithmetic operations on voice commands.
    >>> ['Shoebox', 'authors'] ==> ['W.C. Dersch', 'E.A. Quade']
    >>> ['Shoebox', 'year'] ==> 1961
    >>> ['Eliza', 'authors'] ==> ['Joseph Weizenbaum']
    >>> ['Eliza', 'year'] ==> 1965

The dictionary automatically creates a folder named "my_folder" 
on the local disk. Each key-value pair is stored as 
a separate file within this folder.

If the key is a string, it becomes the filename for the object. 
If the key is a sequence of strings, all strings except the last 
are used to create nested subfolders within the main folder. 
The final string in the sequence serves as the filename for the object, 
which is stored in the deepest subfolder.

Persistent dictionaries only accept sequences of strings as keys. 
Any pickleable Python object can be used as a value. 
Unlike regular Python dictionaries, insertion order is not preserved.

    del my_dictionary
    new_dict = FileDirDict(base_dir="my_folder")
    print("len(new_dict) == ",len(new_dict))

The code above will create a new object named new_dict and then will
print its length: 

    >>> len(new_dict) == 6

The length is 6, because the dictionary was already stored on a disk 
in the "my_folder" directory, which contained 6 pickle files.

Technically, `FileDirDict` saves its content in a folder on a local disk. 
But you can share this folder with other machines 
(for example, using Dropbox or NFS), and work with the same dictionary 
simultaneously from multiple computers (from multiple instances of your program). 
This approach would allow you to use a persistent dictionary in 
a system that is distributed over dozens of computers.

If you need to run your program on hundreds (or more) computers, 
class `S3Dict` is a better choice: it's a persistent dictionary that 
stores its content in an AWS S3 bucket.

    from persidict import S3Dict
    my_cloud_dictionary = S3Dict(bucket_name="my_bucket")

Once created, it can be used as a regular Python dictionary.

## Key Classes

* `SafeStrTuple` - an immutable sequence of URL/filename-safe non-empty strings.
* `PersiDict` - an abstract base class for persistent dictionaries. 
* `FileDirDict` - a persistent dictionary that stores its content 
in a folder on a disk.
* `S3Dict` - a persistent dictionary that stores its content 
in an AWS S3 bucket.

## Key Similarities With Python Built-in Dictionaries

`PersiDict` and its subclasses can be used as regular Python dictionaries. 

* You can use square brackets to get, set, or delete values. 
* You can iterate over keys, values, or items. 
* You can check if a key is in the dictionary. 
* You can check whether two dicts are equal
(meaning they contain the same key-value pairs).
* You can get the length of the dictionary.
* Methods `keys()`, `values()`, `items()`, `get()`, `clear()`
, `setdefault()`, `update()` etc. work as expected.

## Key Differences From Python Built-in Dictionaries

`PersiDict` and its subclasses persist values between program executions, 
as well as make it possible to concurrently run programs 
that simultaneously work with the same instance of a dictionary.

* Keys must be sequences of URL/filename-safe non-empty strings.
* Values must be pickleable Python objects.
* You can constrain values to be an instance of a specific class.
* Insertion order is not preserved.
* You cannot assign initial key-value pairs to a dictionary in its constructor.
* `PersiDict` API has additional methods `delete_if_exists()`, `timestamp()`,
`get_subdict()`, `subdicts()`, `random_keys()`, `newest_keys()`, 
`oldest_keys()`, `newest_values()`, `oldest_values()`, 
`get_params()`, `get_metaparams()`, and `get_default_metaparams()`,
which are not available in native Python dicts.
* You can use KEEP_CURRENT constant as a fake new value 
to avoid actually setting/updating a value. Or DELETE_CURRENT as 
a fake new value to delete the previous value from a dictionary.

## Fine Tuning

`PersiDict` subclasses have a number of parameters that can be used 
to impact behaviour of a dictionary. 

* `base_class_for_values` - A base class for values stored in a dictionary.  
If specified, it will be used to check types of values in the dictionary. 
If not specified (if set to `None`), no type checking will be performed 
and all types will be allowed.
* `file_type` - a string that specifies the type of files used to store objects.
If `file_type` has one of two values: "pkl" or "json", it defines 
which file format will be used by the dictionary to store values. 
For all other values of `file_type`, the file format will always be plain
text. "pkl" or "json" allow to store arbitrary Python objects,
while all other file_type-s only work with str objects; 
it means `base_class_for_values` must be explicitly set to `str` 
if `file_type` is not set to "pkl" or "json".
* `immutable_items` - a boolean that specifies whether items in a dictionary 
can be modified/deleted. It enables various distributed cache optimizations 
for remote storage. True means an append-only dictionary. 
False means normal dict-like behaviour. The default value is False. 
* `digest_len` - a length of a hash signature suffix which `PersiDict` 
automatically adds to each string in a key while mapping the key to 
the address of a value in a persistent storage backend 
(e.g. a filename or an S3 objectname). It is needed to ensure correct work
of persistent dictionaries with case-insensitive (even if case-preserving) 
filesystems, such as MacOS HFS. The default value is 8. 


## How To Get It?

The source code is hosted on GitHub at:
[https://github.com/pythagoras-dev/persidict](https://github.com/pythagoras-dev/persidict) 

Binary installers for the latest released version are available at the Python package index at:
[https://pypi.org/project/persidict](https://pypi.org/project/persidict)

Using uv :
```
uv add persidict
```

Using pip (legacy alternative to uv):
```
pip install persidict
```

## Dependencies

* [jsonpickle](https://jsonpickle.github.io)
* [joblib](https://joblib.readthedocs.io)
* [lz4](https://python-lz4.readthedocs.io)
* [pandas](https://pandas.pydata.org)
* [numpy](https://numpy.org)
* [boto3](https://boto3.readthedocs.io)
* [pytest](https://pytest.org)
* [moto](http://getmoto.org)

## Key Contacts

* [Vlad (Volodymyr) Pavlov](https://www.linkedin.com/in/vlpavlov/)