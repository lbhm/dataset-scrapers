# Extension of the Croissant Specification

## Assumptions

- The `"distribution"` list contains one object with information about the `archive.zip` download file, especially `"contentSize"` in the format `"[size] [unit]"` (e.g., `"contentSize": "7.355 KB"`).
- The records of the `"recordSet"` object each have an `"@id"` field specifying the file name and sometimes include the directory to avoid name conflicts (e.g., `"@id": "cancer+patient+data+sets.csv"`).
- The fields of a record represent columns of a CSV file where the sub-field `"name"` is expected to be a valid column name in the corresponding CSV file.

## Modifications

- We add a `"kaggleRef"` field containing a string with the Kaggle dataset reference (`<user_name>/<dataset_name>`).
- If there is a `recordSet`, we extend the fields of the records depending on the value of the field `"dataType"`.
  - Numeric fields: New key `"histogram"` containing the key `"bins"` and the key `"densities"` each with a list of numbers and new key `"statistics"` with the keys `"count"`, `"mean"`, `"std"`, `"min"`, `"25%"`, `"50%"`, `"75%"` and `"max"` each with a numerical value.
  - Text fields: New key `"n_unique"` with a number and `"most_common"` with 10 keys and a number each referring to the frequency of the key.
  - Boolean fields: A key `count` containing two keys which count the positive and negative occurrences (values are integers).
  - Data fields: The keys `min_date` and `max_date` with a date string in ISO 8601 format as their value and the key `unique_dates` with an integer value.
- Usability score: The key `usability` with a numeric value between 0 and 1 in the top level hierarchy, which is calculated as follows:
  - 1 point for having a license.
  - 1 point for having a subtitle.
  - 1 point for having a description.
  - 1 point for having tags.
  - 1 point for having at least one file description.
  - 1 point for having at least one column description.
  - After collecting all points, we normalize the score by dividing by the number of achievable points (6).
