.PHONY: test clean

PYTHON = . env/bin/activate; exec python

test: env
	. env/bin/activate; nosetests $(NOSEARGS)

clean:
	python setup.py clean
	find pseudonym -type f -name "*.pyc" -exec rm {} \;

coverage: test
	open cover/index.html

env: env/bin/activate
env/bin/activate: requirements.txt setup.py
	test -d env || virtualenv --no-site-packages env
	. env/bin/activate; pip install -U pip wheel
	. env/bin/activate; pip install -r requirements.txt
	touch env/bin/activate
