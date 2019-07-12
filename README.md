## fwf-reader
An extension to [Apache Arrow](https://github.com/apache/arrow) based on their CSV 
reader for reading fixed-width files (tabular data where all fields in a column are 
padded to the same number of bytes) to Arrow tables. Written in C++, with Python bindings. 
Supports various [encodings](http://demo.icu-project.org/icu-bin/convexp).

#### todo
* documentation
* more unit tests
* trailing whitespace in column names and string fields
* ci
* change to less "interesting" build system