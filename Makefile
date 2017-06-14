tests = tests
desc = ''

BUILD = python setup.py install
TEST  = py.test -m "not perf" --cov --cov-report term-missing $(tests)

all:
	$(BUILD)
	$(TEST)

install:
	$(BUILD)

test:
	$(TEST)

perftest:
	py.test -m "perf" $(tests)

release:
	# make sure required variables set via commandline
	ifndef version
		$(error version is not set)
	endif
	ifndef token
		$(error token is not set)
	endif
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
