test = tests
build = python setup.py install && \
		nose2 -C $(test) --coverage-report term-missing --coverage-config .coveragerc

install:
	$(call build,)

release:
	# tag
	git tag $(version)
	# build
	$(call build,)
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
