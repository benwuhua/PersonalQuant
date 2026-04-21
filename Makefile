PYTHON ?= python
CONFIG ?=
PORT ?= 8765
CLI = $(PYTHON) scripts/dev.py

.PHONY: help init-config smoke run dashboard validate backtest archive-diff timeline consolidation-breakout-scanner consolidation-breakout-calibration quant-pipeline wangji-scanner wangji-sacnner cron-run serve clean-pyc
TICKER ?= SH600875
LIMIT ?= 5

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

validate:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) validate

backtest:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) backtest

archive-diff:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) archive-diff

timeline:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) timeline $(TICKER) --limit $(LIMIT)

consolidation-breakout-scanner:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) consolidation-breakout-scanner

wangji-scanner:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) wangji-scanner

wangji-sacnner:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) wangji-sacnner

consolidation-breakout-calibration:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) consolidation-breakout-calibration

quant-pipeline:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) quant-pipeline

cron-run:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) cron-run

serve:
	@$(CLI) $(if $(CONFIG),--config $(CONFIG),) serve --port $(PORT)

clean-pyc:
	@$(CLI) clean-pyc
