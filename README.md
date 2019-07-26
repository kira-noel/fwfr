## fwf-reader
An extension to [Apache Arrow](https://github.com/apache/arrow) based on their CSV 
reader for reading fixed-width files (tabular data where all fields in a column are 
padded to the same number of bytes) to Arrow tables. Written in C++, with Python bindings. 
Supports various [encodings](http://demo.icu-project.org/icu-bin/convexp).

## Installation
We use Conda to manage the build, so you will need an active Conda environment for either installation option.
```
conda create -y -n env
conda activate env
```
### Build from Source
This builds the C++ library (fwfr) from source, cythonizes the Python code and installs the resulting Python module (pyfwfr) into your active Conda environment. Any missing dependencies are picked up by Conda. Unit tests run automatically after installation.
```
git clone https://github.com/kira-noel/fwfr.git
cd fwfr
./install.sh --source
```
Note: also places the C++ library files in the active Conda environment. These are **not** removed by running `conda remove pyfwfr` and must be removed manually.
### Build with Binary Release
```
conda install pyfwfr -c kira-noel
python -m unittest pyfwfr.tests.test_fwf -v
```
Note: also installs the C++ library files in the active Conda environment. These **are** removed by running `conda remove pyfwfr`.
