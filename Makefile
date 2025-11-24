PYTHON ?= ./.venv/bin/python
TMUX_SESSION ?= page_ripper
UVICORN_CMD ?= python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
SCAN_DEBUG_KEEP_SANE_RAW ?= 0
SCAN_DEBUG_RAW_DIR ?= ~/page_the_ripper/scans/debug_raw

# NOTE: Restart uvicorn after any API change so the Raspberry Pi picks up the new routes.

.PHONY: re-ocr
re-ocr:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make re-ocr FILE=path/to.pdf"; \
		exit 1; \
	fi
	$(PYTHON) main.py --re-ocr "$(FILE)"

.PHONY: restart-uvicorn
restart-uvicorn:
	@echo "Restarting page_the_ripper via tmux in the current host..."
	tmux kill-session -t $(TMUX_SESSION) >/dev/null 2>&1 || true
	cd ~/page_the_ripper && SCAN_DEBUG_KEEP_SANE_RAW=$(SCAN_DEBUG_KEEP_SANE_RAW) SCAN_DEBUG_RAW_DIR=$(SCAN_DEBUG_RAW_DIR) tmux new-session -d -s $(TMUX_SESSION) "$(PYTHON) main.py"

.PHONY: tmux-attach
tmux-attach:
	@echo "Attaching to tmux session $(TMUX_SESSION)..."
	if tmux has-session -t $(TMUX_SESSION) 2>/dev/null; then \
		tmux attach -t $(TMUX_SESSION); \
	else \
		echo "Session $(TMUX_SESSION) not found; creating new session."; \
		tmux new-session -s $(TMUX_SESSION); \
	fi

.PHONY: start-uvicorn
start-uvicorn:
	@echo "Starting uvicorn (foreground run)..."
	$(UVICORN_CMD)
