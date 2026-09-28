# Crypto Market Data Recorder

# Use .venv if it exists, otherwise fall back to the system python
PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

SYMBOL ?= xrpusdt
STREAMS ?= depth trade book_ticker
BUFFER ?= 5000
DATA_DIR ?=

TARGETS := help install run depth trades book futures test stats clean clean-data

DATA_DIR_FLAG := $(if $(DATA_DIR),--data-dir $(DATA_DIR),)

# Accept lowercase symbol= / streams= / buffer= as well
ifneq ($(symbol),)
override SYMBOL := $(symbol)
SYMBOL_GIVEN := 1
endif

ifneq ($(streams),)
override STREAMS := $(streams)
endif

ifneq ($(buffer),)
override BUFFER := $(buffer)
endif

ifeq ($(origin SYMBOL),command line)
SYMBOL_GIVEN := 1
endif

# Bare symbols after a target are treated as symbols, so both
#   make run SYMBOL="btcusdt ethusdt"
#   make run btcusdt ethusdt
# work. Only applied when a real target is among the goals, so a mistyped
# target still fails loudly instead of silently doing nothing.
EXTRA_SYMBOLS := $(filter-out $(TARGETS),$(MAKECMDGOALS))

ifneq ($(filter $(TARGETS),$(MAKECMDGOALS)),)
ifneq ($(EXTRA_SYMBOLS),)
ifdef SYMBOL_GIVEN
override SYMBOL := $(SYMBOL) $(EXTRA_SYMBOLS)
else
override SYMBOL := $(EXTRA_SYMBOLS)
endif
$(eval $(EXTRA_SYMBOLS): ; @:)
endif
endif

.DEFAULT_GOAL := help
.PHONY: $(TARGETS)

help: ## List the available targets
	@echo "Usage: make <target> [symbols...] [BUFFER=1000]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:  make run btcusdt ethusdt"
	@echo "           make run SYMBOL=\"btcusdt ethusdt\" BUFFER=1000"
	@echo ""
	@echo "Variables: SYMBOL=$(SYMBOL)  BUFFER=$(BUFFER)"
	@echo "           STREAMS=$(STREAMS)"

install: ## Create .venv and install dependencies
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	.venv/bin/pip install pytest

run: ## Record all selected streams
	$(PYTHON) main.py --symbol $(SYMBOL) --streams $(STREAMS) --buffer-size $(BUFFER) $(DATA_DIR_FLAG)

depth: ## Record the order book depth stream only
	$(PYTHON) main.py --symbol $(SYMBOL) --streams depth --buffer-size $(BUFFER) $(DATA_DIR_FLAG)

trades: ## Record the trade stream only
	$(PYTHON) main.py --symbol $(SYMBOL) --streams trade --buffer-size $(BUFFER) $(DATA_DIR_FLAG)

book: ## Record the bookTicker stream only
	$(PYTHON) main.py --symbol $(SYMBOL) --streams book_ticker --buffer-size $(BUFFER) $(DATA_DIR_FLAG)

futures: ## Record all futures streams (depth, agg_trade, mark_price, liquidation, open_interest)
	$(PYTHON) main.py --symbol $(SYMBOL) --buffer-size $(BUFFER) $(DATA_DIR_FLAG) \
		--streams futures_depth futures_agg_trade futures_mark_price futures_liquidation futures_open_interest

test: ## Run the test suite
	$(PYTHON) -m pytest tests/ -v

stats: ## Summarize recorded data (file count / size)
	@if [ ! -d data ]; then \
		echo "No recordings yet (data/ not found)."; \
	else \
		find data -mindepth 3 -maxdepth 3 -type d | sort | while read -r dir; do \
			printf "%-45s %5s files  %8s\n" "$$dir" \
				"$$(find "$$dir" -name '*.parquet' | wc -l)" \
				"$$(du -sh "$$dir" | cut -f1)"; \
		done; \
		echo "---"; \
		du -sh data; \
	fi

clean: ## Remove __pycache__ directories
	find . -name __pycache__ -type d -not -path './.venv/*' -prune -exec rm -rf {} +

clean-data: ## WARNING: delete all recorded parquet data
	@printf "This will delete data/. Are you sure? [y/N] "; \
	read ans; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
		rm -rf data; \
		echo "data/ deleted."; \
	else \
		echo "Cancelled."; \
	fi
