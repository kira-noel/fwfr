all: 
	@echo "Give more specific target."

wheel: clean
	python setup.py bdist_wheel

clean:
	@rm -rf build
	@rm -f pyfwfr/_fwfr.cpp

cleaner: clean
	@rm -rf dist *egg-info*
	@rm -f *~

cleanest: cleaner
	@rm -f pyfwfr/libfwfr.so
	@rm -rf pyfwfr/include/*
	@rm -rf pyfwfr/__pycache__
	@rm -rf pyfwfr/tests/__pycache__
