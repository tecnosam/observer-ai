PORT      ?= 8000
EXPORTING ?= 0
URL       := http://127.0.0.1:$(PORT)
# macOS has `open`, Linux has `xdg-open`. Override with OPEN=true to not open a browser.
OPEN      ?= $(shell command -v open || command -v xdg-open || echo true)

.PHONY: chat questions grade test smoke corpus

# Start the agent with ADK's chat UI and open it in the browser once it is up.
# Prints a few sample questions from the corpus first.
# Ctrl-C stops it. `make chat EXPORTING=1` also sends each message's trace to
# the Observer engine; `make chat PORT=8001` uses another port.
chat:
	@uv run python scripts/sample_questions.py
	@( for i in $$(seq 1 120); do \
	     if curl -s -o /dev/null $(URL)/list-apps; then $(OPEN) "$(URL)/dev-ui/?app=observer_chat"; exit 0; fi; \
	     sleep 0.5; \
	   done; \
	   echo "chat: the server did not come up on $(URL)" >&2 ) & \
	OBSERVER_EXPORTING=$(EXPORTING) uv run adk web chat --port $(PORT)

# Show another few sample questions, for example while the chat is running.
questions:
	@uv run python scripts/sample_questions.py

# HotpotQA paragraphs for the chat agent to search (writes chat/corpus.json
# and the sample questions in chat/questions.json).
corpus:
	uv run --with datasets python scripts/build_chat_corpus.py

# Run the agent on every chat question once and record whether it was right,
# so `make chat` can show both kinds. Needs Ollama; about ten minutes.
grade:
	uv run python scripts/grade_questions.py

# Offline, no Ollama needed.
test:
	uv run pytest

# One two-hop question against Ollama. `make smoke EXPORTING=1` exports its trace.
smoke:
	uv run python scripts/smoke.py $(if $(filter 1 true,$(EXPORTING)),--exporting)
