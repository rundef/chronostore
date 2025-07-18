.PHONY: tests

TESTS_PATH?=tests

tests:
	PYTHONPATH=. pytest -s -vv $(TESTS_PATH)
