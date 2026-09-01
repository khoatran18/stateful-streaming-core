# Code Style Rules

> Part of: [CLAUDE.md](../../CLAUDE.md) | [Project Overview](../../docs/PROJECT_OVERVIEW.md)

## Comments

- Write comments in **English only**.
- Use plain English sentences — no Javadoc HTML tags (`<ul>`, `<li>`, `<p>`, `<b>`, `{@code}`, `{@link}`, etc.).
- Use `//` for all comments in Java, both class-level and method-level. Reserve `/** */` only when a tool or framework explicitly reads Javadoc (e.g. public library API). This project does not expose a public API, so `/** */` is not used.
- Comment the **why and the contract**, not the what. Avoid restating what the code already says.
- Keep comments concise. One or two lines is almost always enough for a method. Longer explanations belong in the design doc, not inline.
- For parameters and return values, describe them inline in the comment block above the method, not as `@param` / `@return` tags.

### Comment structure for methods

```java
// Short description of what the method does and why it exists.
// param1 - meaning, param2 - meaning, ...
// Returns ... (only if non-obvious)
public void doSomething(int param1, String param2) {
```

### Comment structure for classes

```java
// One or two sentence description of the class responsibility.
//
// Any important design decisions or non-obvious constraints go here.
// Bullet points with plain dashes if needed.
public class Foo {
```

### Inline comments

- Use inline `//` comments only for non-obvious logic, not for code that reads naturally.
- For branching sections inside a method, a short `// step description` label is fine.

---

## Naming

- Class names: PascalCase, noun or noun phrase.
- Method names: camelCase, verb or verb phrase.
- Constants: UPPER_SNAKE_CASE.
- Variables and fields: camelCase, descriptive, no abbreviations unless universally understood (e.g. `id`, `url`, `vnd`).

---

## General Style

- Keep methods short. If a private method needs more than one block of explanation comments, it is probably doing too much.
- Prefer explicit over clever. Avoid one-liners that sacrifice readability.
- All business constants (thresholds, window sizes, topic names) must be loaded dynamically via Broadcast State or configuration, not hardcoded.
- Operator UIDs must be set explicitly on all Flink operators to support Savepoint recovery.

---

## Logging

- **Never use `System.out` or `System.err`** for any logging. All output must go through the SLF4J logger backed by `logback.xml`.
- Declare one static logger per class:
  ```java
  private static final Logger LOG = LoggerFactory.getLogger(MyClass.class);
  ```
- All log messages must be in **English**.
- Use parameterized placeholders (`{}`) instead of string concatenation to avoid unnecessary object allocation:
  ```java
  // correct
  LOG.info("Schema parsed: key={} columns={}", key, columns.size());

  // wrong
  LOG.info("Schema parsed: key=" + key + " columns=" + columns.size());
  ```

### Log level guide

| Level   | When to use |
|---------|-------------|
| `ERROR` | Unrecoverable failure; job or operator cannot proceed (e.g. config missing, unhandled exception). |
| `WARN`  | Recoverable but abnormal situation requiring attention (e.g. DLQ routing, missing env var, null payload). |
| `INFO`  | Significant lifecycle milestones: job start/stop, config loaded, schema broadcast updated, pipeline step built. |
| `DEBUG` | Per-message or per-call detail useful during development: event customerId, byte count, resolved source/version. Only enabled locally. |
| `TRACE` | Per-field or tight-loop detail (e.g. each DFS step, each cast). Only enabled when debugging a specific bug. |

### What to log (examples)

```java
// INFO — pipeline milestone
LOG.info("Starting Flink Job [{}] | bootstrap={} | topics={}", appName, bootstrap, topics);

// INFO — state update
LOG.info("Schema broadcast state updated: key={} totalFields={}", key, schema.getTotalFields());

// WARN — event routed to DLQ
LOG.warn("No schema found for key={}, routing event to DLQ", key);

// WARN — recoverable config issue
LOG.warn("Env var '{}' not set, keeping placeholder as-is", varName);

// DEBUG — per-event trace
LOG.debug("Validated event: customerId={} eventTime={} fieldCount={}", id, time, count);

// ERROR — unrecoverable, with exception
LOG.error("Failed to load config file: {}", fileName, e);
```

### What not to log

- Do not log full event payloads at INFO or WARN — use DEBUG or TRACE only.
- Do not log inside tight inner loops at INFO or WARN (e.g. inside DFS per-field iteration).
- Do not log after every getter/setter call.

---

## Verification

- **Do not run builds to verify changes.** Self-check the code logic and consistency manually. The user runs tests themselves.

---

## Markdown Files

- Every `.md` file written or edited must stay at or below **200 lines**. If content exceeds this, trim or split into referenced sub-documents.
