# Stateful Streaming Core — Claude Context

## Required reading before making changes

- [Project Architecture & Logic](docs/PROJECT_OVERVIEW.md)
  Covers the four core pillars (ring buffer aggregation, broadcast state hot-reload,
  inverted index filtering, fault tolerance), the processing pipeline, and all coding constraints.

- [Development & Local Setup Guide](docs/setup/LOCAL_SETUP.md)
  Requirements, IDE configuration (VM Options for Java 21), build commands, and local infrastructure setup.

- [Code Style Rules](.claude/rules/code-style.md)
  Covers comment format (plain English, no HTML/Javadoc tags), naming conventions,
  and general style rules that apply to all Java files in this project.

## Module docs

- [Data Generator & Rule Generator](data-generator/src/main/java/vdf/vdt/streaming/generator/README.md)
  Schema design (200-field CDP schema, 4 categories), data generation logic,
  rule generation logic (4 expression types, window naming format), and schema publishing.
