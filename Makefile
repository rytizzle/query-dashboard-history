CATALOG  ?= $(shell awk '/^  catalog:/{getline; sub(/^.*default: */, ""); gsub(/"/, ""); print; exit}' databricks.yml)
SCHEMA   ?= $(shell awk '/^  schema:/{getline; sub(/^.*default: */, ""); gsub(/"/, ""); print; exit}' databricks.yml)
MV_SCHEMA ?= $(shell awk '/^  metric_view_schema:/{getline; sub(/^.*default: */, ""); gsub(/"/, ""); print; exit}' databricks.yml)
TARGET   ?= dev
PROFILE  ?=

PROFILE_FLAG := $(if $(PROFILE),--profile $(PROFILE),)

.PHONY: help render render-dev render-prod validate deploy run clean

help:
	@echo "Targets:"
	@echo "  render      Render Genie space templates -> genie_space/rendered/"
	@echo "              Vars: CATALOG=$(CATALOG) SCHEMA=$(SCHEMA) MV_SCHEMA=$(MV_SCHEMA)"
	@echo "  validate    databricks bundle validate -t \$$TARGET"
	@echo "  deploy      render + databricks bundle deploy -t \$$TARGET"
	@echo "  run         databricks bundle run workspace_inventory -t \$$TARGET"
	@echo "  clean       Remove genie_space/rendered/"
	@echo ""
	@echo "  TARGET=$(TARGET)  PROFILE=$(PROFILE)"

render:
	python3 scripts/render_genie_space.py --catalog "$(CATALOG)" --schema "$(SCHEMA)" --mv-schema "$(MV_SCHEMA)"

render-dev:
	$(MAKE) render TARGET=dev

render-prod:
	$(MAKE) render TARGET=prod CATALOG=$(CATALOG) SCHEMA=$(SCHEMA)

validate:
	databricks bundle validate -t $(TARGET) $(PROFILE_FLAG)

deploy: render
	databricks bundle deploy -t $(TARGET) $(PROFILE_FLAG) --var catalog=$(CATALOG) --var schema=$(SCHEMA)

run:
	databricks bundle run workspace_inventory -t $(TARGET) $(PROFILE_FLAG)

clean:
	rm -rf genie_space/rendered/
