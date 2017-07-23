tests = tests
desc = ''
# Use this option to show full stack trace for errors
#pytestopts = '--full-trace'
# Use these options to measure test coverage (requires python >= 3.5 and 
# pytest-cov package)
pytestopts = '--cov --cov-report term-missing'

BUILD = python setup.py install
TEST  = python -m pytest -m "not perf" $(pytestopts) $(tests)

all:
	$(BUILD)
	$(TEST)

install:
	$(BUILD)

test:
	$(TEST)

perftest:
	py.test -m "perf" $(tests)

clean:
	rm -Rf __pycache__
	rm -Rf **/__pycache__/*
	rm -Rf dist
	rm -Rf build
	rm -Rf *.egg-info

release:
	$(clean)
	# tag
	git tag $(version)
	# build
	$(BUILD)
	$(TEST)
	python setup.py sdist bdist_wheel
	# release
	twine upload dist/xphyle-$(version).tar.gz
	# push new tag after successful build
	git push origin --tags
	# create release in GitHub
	curl -v -i -X POST \
		-H "Content-Type:application/json" \
		-H "Authorization: token $(token)" \
		https://api.github.com/repos/jdidion/xphyle/releases \
		-d '{"tag_name":"$(version)","target_commitish": "master","name": "$(version)","body": "$(desc)","draft": false,"prerelease": false}'

docs:
	make -C docs api
	make -C docs html

readme:
	pandoc --from=markdown --to=rst --output=README.rst README.md

lint:
	pylint xphyle
