---
name: Bug report
about: Report a bug or unexpected behavior
title: ''
labels: bug
assignees: ''
---

## Bug Description
A clear and concise description of what the bug is.

## Steps to Reproduce
1. Run command `bq-schema-gen ...`
2. With input data: `...`
3. See error

## Expected Behavior
What you expected to happen.

## Actual Behavior
What actually happened.

## Sample Input
```json
{"example": "data"}
```

## Expected Output
```json
[{"name": "example", "type": "STRING", "mode": "NULLABLE"}]
```

## Actual Output
```
Error or incorrect output here
```

## Environment
- OS: [e.g., macOS 14.0, Ubuntu 22.04, Windows 11]
- Version: [e.g., 0.1.0 - run `bq-schema-gen --version`]
- Installation method: [cargo install, homebrew, binary download]

## Additional Context
Add any other context about the problem here.
