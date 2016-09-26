# Overview

## Summary

Legion is a Hadoop MapReduce tool that turns big, messy data sources into clean, normalized flat files ready for ingestion into relational tables in a data warehouse (e.g., Postgres COPY).

In its current form, it is particularly suited to analytical data warehouses (as opposed to transactional databases) where a trivial amount of data loss is acceptable. Its current bias is towards skipping rather than fixing bad records, though additional tools to change this are in development.

Legion currently supports:

* Input data in JSON (one object per line) or CSV formats
* One large input file or hundreds of thousands of tiny input files
* Gracefully ignoring corrupt files rather than throwing an exception
* Skipping or replacing corrupt data fields (e.g., character data in an integer column)
* Splitting a single input (e.g., a JSON object with an array) into multiple, normalized outputs (e.g., a table for the objects, and another for the array elements)
* Handling compressed data with improper file extensions
* Re-ordering CSV columns for consistency

Hadoop developers may also be interested in leveraging individual Legion components (e.g., input formats) in other MapReduce jobs.

## Origins

Legion was originally developed at Republic Wireless, where we've applied it to over a dozen problems. These include:

* Combining hundreds of thousands of small CSV files uploaded to our servers every day by our Android app out in the field. Legion lets us deal with corrupt files (either unreadable or containing bad data), harmonize data streams from old and new versions of our app, and deal with incorrect file extensions.
* Normalizing JSON log data from our switching servers. Legion lets us split a single stream of JSON objects containing arrays of arrays into several different files that fit our normalized database schema.

# Usage

## The Basics

Legion comes with a pre-configured job that handles all of the use cases described above. You can execute this job by calling the Hadoop `jar` command, with the Legion jar as the target jar and `com.rw.legion.DefaultJob` as the `mainClass`.

The default Legion job expects three additional arguments of its own:

1. Input location
2. Output location
3. Location of Legion "objective" file

All together, you get something like this:

```
hadoop jar /path/to/Legion.jar com.rw.legion.DefaultJob /in /out /path/to/objective.json
```

## Objective files

A Legion objective file is a JSON document that specifies:

* `inputDataType` - Currently either 'CSV' or 'JSON'.
* `codecOverride` - The canonical class name of the Hadoop codec class to use for reading input, if you want to override default behavior. Optional.
* `combineFiles` - Whether or not Legion should combine input files. Use `true` for small files, `false` for large files.
* `maxCombinedSize` - Required if `combineFiles` is true. Controls `setMaxInputSplitSize()` for `CombineLegionInputFormat` (see [CombineFileInputFormat](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/mapreduce/lib/input/CombineFileInputFormat.html)).
* `outputTables` - An array of objects specifying output tables that Legion should create.

Each object in the `outputTables` list should specify:

* `title` - The title of the table, for differentiating output files.
* `indexes` - An array listing the names of indexes to be used for this table. Optional. (See section on indexes below.)
* `columns` - An array of objects that specifies what columns this table should contain.

Each object in the `columns` list should specify:

* `key` - The key (CSV column header or modified JsonPath) used to find the data for this column in the input data.
* `dataType` - Currently either 'String', 'Int', 'Float', 'Scientific', or 'Boolean'. Legion will not output rows containing a field that does not match its column type.
* `regex` - For string columns, a regex that the string must match. Legion will reject rows containing fields that don't match their regexes. Optional.
* `allowNulls` - Whether null values should be allowed in this column. Defaults to true.
* `absentAsNull` - Whether fields that are absent from the input data (e.g., column missing from a CSV) should be treated as null values. If false, Legion will reject the row. Defaults to true.
* `nullSubstitute` - A string to substitute for null values. Optional.

## JsonPaths

Legion uses a modified (highly simplified) version of JsonPaths. Essentially, the root of your JSON object is `$`. You can select attributes or sub-attributes by using dots. For example, `$.name` or `$.compensation.salary`. If there's an array in the JSON object, you can access elements using square brackets. For example, `$.states[23].capital`. That's all there is to it.

## Simple use case

Suppose you have a CSV that looks like this:

~~~
first_name,last_name,salary
yolanda,jones,50000
john,smith,ILOVEJUSTINBIEBER
chris,brown,12000000
~~~

And you have a database table that looks like this:

~~~SQL
create table employees (
    last_name varchar(255),
    first_name varchar(255),
    salary int
);
~~~

You might be interested in accomplishing a couple of things to prepare this data for ingestion. First, you might want to re-organize the data so that employee last names come before first names. Second, you might want to throw out salary values that don't match the integer data type, so that your database import won't crash.

A simple Legion objective to accomplish that would look like this:

~~~JSON
{
    "inputDataType": "CSV",
    "outputTables":
    [
        {
            "title": "employee",
            "columns":
            [
                {
                    "key": "last_name",
                    "dataType": "String"
                },
                {
                    "key": "first_name",
                    "dataType": "String"
                },
                {
                    "key": "salary",
                    "dataType": "Int"
                }
            ]
        }
    ]
}
~~~

Legion will generate output that looks like this:

~~~
jones,yolanda,50000
brown,chris,12000000
~~~

Clean and ready for import!

## Indexes for table normalization

Suppose you have a file full of JSON objects that look like this (pretty-printed here for illustration purposes):

~~~JSON
{
    "inventor_id": 1,
    "first_name": "john",
    "middle_name": "avery",
    "last_name": "whittaker",
    "patents": [
        {
            "patent_no": 12345,
            "title": "super scooper"
        },
        {
            "patent_no": 67890,
            "title": "instant freezer"
        }
    ]
}
~~~

If you were loading this data into a relational database, you'd likely want to have a table of inventors and a table of patents, with each record in the patent table containing a foreign key (inventor_id) linking it to its inventor. This can be accomplished using "indexes" in Legion.

A Legion index is simply a named placeholder for a numerical value that might appear in the key for certain repeated data points. It is surrounded by angle brackets in the actual key definition so Legion can identify and use the index. Keys must also be listed in the table definition itself, as mentioned above.

In the above example, since Legion uses JsonPath notation to determine keys for JSON attributes, you'll end up with keys for patent numbers that look like `$.patents[0].patent_no` and `$.patents[1].patent_no`. If you wanted to use an index to create a generic string that would match both of these keys, you could use `$.patents[<patentIndex>].patent_no` and include `patentIndex` in the list of indexes for your patents table.

If that's all clear as mud, this example should help:

~~~JSON
{
    "inputDataType": "JSON",
    "outputTables":
    [
        {
            "title": "inventor",
            "columns":
            [
                {
                    "key": "$.inventor_id",
                    "dataType": "Int"
                },
                {
                    "key": "$.first_name",
                    "dataType": "String"
                },
                {
                    "key": "$.middle_name",
                    "dataType": "String"
                },
                {
                    "key": "$.last_name",
                    "dataType": "String"
                }
            ]
        },
        {
            "title": "patent",
            "indexes": ["patentIndex"],
            "columns":
            [
                {
                    "key": "$.inventor_id",
                    "dataType": "Int"
                },
                {
                    "key": "$.patents[<patentIndex>].patent_no",
                    "dataType": "Int"
                },
                {
                    "key": "$.patents[<patentIndex>].title",
                    "dataType": "String"
                }
            ]
        }
    ]
}
~~~

This will produce two CSVs. The first will have a prefix of "inventor" and look like this:

~~~
1,john,avery,whittaker
~~~

The second will have a prefix of "patent" and look like this:

~~~
1,12345,super scooper
1,67890,instant freezer
~~~

That's it! You can go as deep as you want with the indexes, and Legion should hold up!

# Using legion components

You can certainly use various components of Legion in your own projects. Here's a quick description of what a few key players do, but you may have just as much luck browsing the source and looking at the comments / Javadocs.

* `LegionRecord` - A glorified hash map that's the basis of everything Legion does. Links data keys to the values they're associated with.
* `LegionInputFormat` - Reads input data and produces `NullWritable` keys and `LegionRecord` values.
* `CombineLegionInputFormat` - `CombineFileInputFormat` equivalent for `LegionInputFormat`.
* `LegionRecordReader` - `RecordReader` for `LegionInputFormat`.
* `LegionObjective` - Object representing a Legion Objective (see above). Contains `OutputTable`s and `OutputColumn`s.

# Future development

We've got all kinds of ideas for feature additions and improvements for Legion. These include:

* Replace bad field data with null or a default value, rather than rejecting the whole record
* Check data against a provided list of valid values, rather than a regex
* Support data type conversion (e.g., scientific notation to standard float)
* Allow CSV input files to be splittable (in the Hadoop sense of the word)
* Support URL query string input data
* Etc.

Please feel free to contribute if you are interested/able!

# Legal

## License

~~~
Copyright © 2016 Republic Wireless

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
~~~

## Credits

This product is derived in part from software developed as part of The Apache Software Foundation's Hadoop project (http://hadoop.apache.org/) and released under the Apache License, Version 2.0.