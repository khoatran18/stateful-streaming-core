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

## Verification

- **Do not run builds to verify changes.** Self-check the code logic and consistency manually. The user runs tests themselves.

---

## Markdown Files

- Every `.md` file written or edited must stay at or below **200 lines**. If content exceeds this, trim or split into referenced sub-documents.
