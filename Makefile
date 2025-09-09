# Makefile (TAB-indented recipes; compatible with macOS make)
.DEFAULT_GOAL := help

# Defaults (override on the command line)
PAIR  ?= BTC-USD
START ?= 2025-09-07T00:00:00Z
END   ?= 2025-09-08T00:00:00Z
H     ?= 2h
IMB   ?= 0.20
MINC  ?= 2
TP    ?= 0.02
DD    ?= -0.01
WHALE ?= 10000

# Derive label name from TP/DD (e.g., 0.02,-0.01 -> label_bull_2h_2pct_dd1pct)
TP_PCT := $(shell python3 -c "print(int(abs(float('$(TP)'))*100))")
DD_PCT := $(shell python3 -c "print(int(abs(float('$(DD)'))*100))")
LABEL  := label_bull_$(H)_$(TP_PCT)pct_dd$(DD_PCT)pct


help: ## Show this help
	@awk 'BEGIN{FS":.*##"; printf "\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##/{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fetch: ## Download trades between START and END
	node src/fetch_trades_node/fetch_trades_by_time.js $(PAIR) $(START) $(END)

ingest: ## Build partitioned Parquet from all raw JSONL (deduped)
	python src/py/ingest_to_parquet.py


build: ## Build 15min features + whale stats
	python src/py/build_dataset.py --pair $(PAIR) --bar 15min --whale_usd $(WHALE)

label: ## Create labels from TP/DD for all horizons
	python src/py/labeler.py --pair $(PAIR) --tp_pct $(TP) --dd_guard $(DD)


sweep: ## Grid search whale thresholds for a horizon
	python src/py/report_sweep.py --pair $(PAIR) --horizon $(H) --imb_range 0.10,0.15,0.20,0.25,0.30 --min_counts 1,2,3,4,5


backtest: ## Backtest baseline with thresholds against auto-named label
	python src/py/backtest_baselines.py --pair $(PAIR) --imb $(IMB) --min_count $(MINC) --horizon $(H) --label $(LABEL)

all: ## Ingest → build → label → sweep → backtest
	$(MAKE) ingest
	$(MAKE) build  PAIR=$(PAIR) WHALE=$(WHALE)
	$(MAKE) label  PAIR=$(PAIR) TP=$(TP) DD=$(DD)
	$(MAKE) sweep  PAIR=$(PAIR) H=$(H)
	$(MAKE) backtest PAIR=$(PAIR) H=$(H) IMB=$(IMB) MINC=$(MINC) TP=$(TP) DD=$(DD)
