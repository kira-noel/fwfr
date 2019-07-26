all: wheel

wheel: clean
	@echo "building wheel"
	@python setup.py bdist_wheel

wheel-quiet: clean
	@echo "building wheel -- quiet"
	@python setup.py bdist_wheel 1>/dev/null

tar: clean
	@echo "building tar"
	@python setup.py bdist --formats=gztar

clean:
	@rm -rf build
	@rm -f pyfwfr/_fwfr.cpp

cleaner: clean
	@rm -rf dist *egg-info*
	@rm -f *~

cleanest: cleaner
	@rm -rf pyfwfr/include/*
	@rm -rf pyfwfr/__pycache__
	@rm -rf pyfwfr/tests/__pycache__
