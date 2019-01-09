module = xphyle
#pytestops = "--full-trace"
#pytestops = "-v -s"
repo = jdidion/$(module)
desc = Release $(version)
tests = tests
desc = ''
# Use this option to show full stack trace for errors
#pytestopts = "--full-trace"

BUILD = python setup.py install
TEST  = py.test -m "not perf" -vv --cov --cov-report term-missing $(pytestopts) $(tests)

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
	rm -Rf .pytest_cache
	rm -Rf .coverage

release:
	echo "Releasing version $(version)"
	$(clean)
	# tag
	git tag $(version)
	# build
	$(BUILD)
	$(TEST)
	python setup.py sdist bdist_wheel
	# release
	python setup.py sdist upload -r pypi
	# push new tag after successful build
	git push origin --tags
	# create release in GitHub
	curl -v -i -X POST \
		-H "Content-Type:application/json" \
		-H "Authorization: token $(token)" \
		https://api.github.com/repos/$(repo)/releases \
		-d '{ \
		  "tag_name":"$(version)", \
		  "target_commitish": "master", \
		  "name": "$(version)", \
		  "body": "$(desc)", \
		  "draft": false, \
		  "prerelease": false \
		}'

docs:
	make -C docs api
	make -C docs html

readme:
	pandoc --from=markdown --to=rst --output=README.rst README.md

lint:
	pylint $(module)
