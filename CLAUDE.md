# Stateful Streaming Core — Claude Context

## Required reading before making changes

- [Project Architecture & Logic](docs/PROJECT_OVERVIEW.md)
  Covers the four core pillars (ring buffer aggregation, broadcast state hot-reload,
  inverted index filtering, fault tolerance), the processing pipeline, and all coding constraints.

- [Code Style Rules](.claude/rules/code-style.md)
  Covers comment format (plain English, no HTML/Javadoc tags), naming conventions,
  and general style rules that apply to all Java files in this project.

## Module docs

- [Data Generator & Rule Generator](data-generator/src/main/java/vdf/vdt/streaming/generator/README.md)
  Schema design (200-field CDP schema, 4 categories), data generation logic,
  rule generation logic (4 expression types, window naming format), and schema publishing.
