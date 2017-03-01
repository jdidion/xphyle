tests = tests

BUILD = python setup.py install
TEST  = py.test --cov --cov-report term-missing $(tests)

all:
	$(BUILD)
	$(TEST)

install:
	$(BUILD)

test:
	$(TEST)

release:
	# tag
	git tag $(version)
	# build
	$(BUILD)
	$(TEST)
	python setup.py sdist bdist_wheel
	# release
	twine register dist/xphyle-$(version).tar.gz
	twine upload dist/xphyle-$(version).tar.gz
	# push new tag after successful build
	git push origin --tags

docs:
	make -C docs api
	make -C docs html

readme:
	pandoc --from=markdown --to=rst --output=README.rst README.md

lint:
	pylint xphyle
