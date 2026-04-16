PYTHON ?= python
CONFIG ?=
PORT ?= 8765

ifeq ($(strip $(CONFIG)),)
CONFIG_ENV =
else
CONFIG_ENV = PERSONALQUANT_CONFIG=$(CONFIG)
endif

.PHONY: help init-config smoke run dashboard serve clean-pyc

help:
	@echo "PersonalQuant common commands"
	@echo "  make init-config              # create config/config.local.yaml from sample if missing"
	@echo "  make smoke                    # compile + import smoke test"
	@echo "  make run                      # run the full weekly pipeline"
	@echo "  make dashboard                # rebuild dashboard_data.json only"
	@echo "  make serve PORT=8765          # serve local dashboard"
	@echo "  make clean-pyc                # remove Python bytecode caches"
	@echo ""
	@echo "Optional overrides:"
	@echo "  PYTHON=python3.11"
	@echo "  CONFIG=config/config.local.yaml"

init-config:
	@if [ -f config/config.local.yaml ]; then \
		echo "config/config.local.yaml already exists"; \
	else \
		cp config/config.sample.yaml config/config.local.yaml; \
		echo "created config/config.local.yaml from sample"; \
	fi

smoke:
	@$(PYTHON) -m compileall src scripts
	@$(CONFIG_ENV) $(PYTHON) -c "import sys; from pathlib import Path; root = Path.cwd(); sys.path.insert(0, str(root / 'src')); import ashare_platform.announcements, ashare_platform.config, ashare_platform.dashboard, ashare_platform.io_utils, ashare_platform.priority, ashare_platform.qlib_pipeline, ashare_platform.summarizer, ashare_platform.watchlist; print('smoke ok')"

run:
	@$(CONFIG_ENV) $(PYTHON) scripts/run_weekly_pipeline.py

dashboard:
	@$(CONFIG_ENV) $(PYTHON) scripts/build_dashboard_data.py

serve:
	@$(PYTHON) scripts/serve_dashboard.py --port $(PORT)

clean-pyc:
	@find src scripts -type d -name '__pycache__' -prune -exec rm -rf {} +
	@find src scripts -type f -name '*.pyc' -delete
	@echo "python caches cleaned"
