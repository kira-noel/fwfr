# fwf-reader
An extension to [Apache Arrow](https://github.com/apache/arrow) based on their CSV 
reader for reading fixed-width files (tabular data where all fields in a column are 
padded to the same number of bytes) to Arrow tables. Written in C++, with Python bindings. 
Supports various [encodings](http://demo.icu-project.org/icu-bin/convexp).

Although the project is standalone, it is intended for use with 
[Artemis](https://gitlab.k8s.cloud.statcan.ca/stcdatascience/artemis).

# Installation
### Installation from source
Install and build everything from source into your active Conda environment. Any
missing dependencies are installed in Conda. Unit tests run automatically after 
installation.
```
conda create -n env
conda activate env
git clone https://gitlab.k8s.cloud.statcan.ca/stcdatascience/fwfr.git
cd fwfr
./install.sh --source
```
If you want to use the C++ library without Python, the installation script also installs 
libfwfr.so and headers in $CONDA\_PREFIX/{lib,include/fwfr}.

Note: if modifying setup.py (the file used to build the Python bindings), note that you cannot use distutils. Wheel uses these. Instead, use setuptools.

### Installation from pre-compiled binaries (not recommended)
Download the module using the most recent binaries. Download the local-channel.tar.gz
archive from the most recent pipeline (job: build-recipe) using the GitLab web interface.
This will install the Python and C++ components in your activate Conda environment.
Missing dependencies are gathered automatically. Unit tests run after installation.
```
conda create -y -n env
conda activate env
git clone https://gitlab.k8s.cloud.statcan.ca/stcdatascience/fwfr.git
cd fwfr
cp PATH/TO/LOCAL-CHANNEl.TAR.GZ ./
./install.sh --conda
```

# Reference
## pyfwfr
Main Python module.

#### ParseOptions
Options for parsing FWF data.

**field_widths**: int list, required<br>
The number of bytes in each field in a column of FWF data.

**ignore_empty_lines**: bool, optional (default True)<br>
Whether empty lines are ignored in FWF input.

**skip_columns**: int list, optional (default empty)<br>
Indexes of columns to skip on read-in.
```python
import pyfwfr as pf
parse_options = pf.ParseOptions([6, 6, 6, 4], ignore_empty_lines=True, [0, 1, 6])
parse_options.field_widths  # displays [6, 6, 6, 4]
parse_options.field_widths = [4, 4, 4, 4]
parse_options.field_widths  # displays [4, 4, 4, 4]
```

#### ReadOptions
Options for reading FWF data.

**encoding**: string, optional (default "")<br>
The encoding on the input data, if any. **General note:** the encoding names are flexible (case, dashes, etc.).
Look [https://demo.icu-project.org/icu-bin/convexp](here) for a list of supported aliases. **EBCDIC note**: must 
append ',swaplfnl' ('cp1047' --> 'cp1047,swaplfnl'). EBCDIC encodings swap the order of carriage return and newline.

**use_threads**: bool, optional (default True)<br>
Whether to use multiple thread to accelerate reading.

**block_size**: int, optional (default 1MB)<br>
How many bytes to process at a time from the input stream. This will determine multi-threading granularity as well as the size of individual chunks in the table

**skip_rows**: int, optional (deafult 0)<br>
Number of rows to skip at the beginning of the input stream.

**column_names**: list, optional<br>
Column names (if empty, will attempt to read from first row after 'skip\_rows').
```python
import pyfwfr as pf
read_options = pf.ReadOptions(encoding="cp500,swaplfnl", use_threads=True, block_size=1024)
```

#### ConvertOptions
Options for converting FWF data.

**column_types**: dict, optional<br>
Map column names to column types (disables type inferencing on those columns.

**is_cobol**: bool, optional (deafult False)<br>
Whether to check for COBOL-formatted numeric types. Uses values provided in pos\_values and neg\_values
for the conversion.

**pos_values**: dict, optional (default mapping provided)<br>
COBOL values for interpreting positive numeric values.

**neg_values**: dict, optional (default mapping provided)<br>
COBOL values for interpreting negative numeric values.

**null_values**: list, optional<br>
A sequence of string that denote nulls in the data (defaults are appropriate in most cases).

**true_values**: list, optional<br>
A sequence of string that denote true booleans in the data (defaults are appropriate in most cases).

**false_values**: list, optional<br>
A sequence of string that denote true booleans in the data (defaults are appropriate in most cases).

**strings_can_be_null**: bool, optional (default False)<br>
Whether string/binary columns can have null values. If true, then strings in null\_values are considered null for string columns. If false, then all strings are valid string values.
```python
import pyfwfr as pf
convert_options = pf.ConvertOptions()
```

#### read\_fwf
Read a Table from a stream of FWF data. Must set parse\_options.field\_widths!

**input_file**: string, path or file-like object<br>
**parse_options**: fwf.ParseOptions, required<br>
**read_options**: fwf.ReadOptions, optional<br>
**convert_options**: fwf.ConvertOptions, optional<br>
**memory_pool**: MemoryPool, optional<br>
```python
import pyfwfr as pf
parse_options = pf.ParseOptions([6, 6, 6, 4])
read_options = pf.ReadOptions(encoding="big5")
table = pf.read_fwf(filename, parse_options, read_options=read_options)
```

#### get\_library\_dir
Return absolute path to libfwfr.so, the C++ base library.

## Unit tests
Current included tests:
* test\_big: threaded-read a large (big enough to use chunker) UTF8 dataset.
* test\_big\_encoded: threaded-read a large (big enough to use chunker) big5-encoded dataset.
* test\_cobol: ensure column type and conversion for numeric COBOL-formatted dataset.
* test\_convert\_options: set and get all ConvertOptions.
* test\_header: parse header for column names.
* test\_no\_header: get column names from column\_names option instead of first row.
* test\_nulls\_bools: read null and boolean values with leading/trailing whitespace.
* test\_parse\_options: set and get all ParseOptions.
* test\_read\_options: set and get all ReadOptions.
* test\_serial\_read: read table serially.
* test\_skip\_columns: have the parser skip the specified columns.
* test\_small: threaded-read a small UTF8 dataset.
* test\_small\_encoded: threaded-read a small big5-encoded dataset.

```
python -m unittest pyfwfr.tests.test_fwf -v
```
