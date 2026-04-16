PYTHON ?= python
CONFIG ?=
PORT ?= 8765
CLI = $(PYTHON) scripts/dev.py

.PHONY: help init-config smoke run dashboard serve clean-pyc

help:
	@$(CLI) --help

init-config:
	@$(CLI) init-config

smoke:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) smoke

run:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) run

dashboard:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) dashboard

serve:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) serve --port $(PORT)

clean-pyc:
	@$(CLI) clean-pyc
