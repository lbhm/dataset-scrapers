## Croissant standard extension

### Assumptions

- in the `"distribution"` list, there exists one object which contains information about the `archive.zip` download file, especially `"contentSize"` in the format `"[size] [unit]"` e.g. `"contentSize": "7.355 KB"`
- the records of the `"recordSet"` object each have an `"@id"` object (key property) referring to the name of csv files and sometimes include the directory to avoid name conflicts e.g. `"@id": "cancer+patient+data+sets.csv"`
- the fields of a record represent columns of a csv file where an object `"name"` is expected to be a valid column name in the corresponding csv file
### Modifications

- we add a `"kaggleRef"` object containing a string to the top level object
- if there is a recordSet, we modify the fields of the records depending on the data type of the field `"dataType"`
    - for numeric fields: New key `"histogram"` containing the key `"bins"` and the key `"densities"` each with a list of numbers and new key `"statistics"` with the keys `"count"`, `"mean"`, `"std"`, `"min"`, `"25%"`, `"50%"`, `"75%"` and `"max"` each with a numerical value
    - for text fields: New key `"n_unique"` with a number and `"most_common"` with 10 keys and a number each referring to the frequency of the key
    - for boolean fields: Two keys `"n_true"` and `"n_false"` each with a integer value