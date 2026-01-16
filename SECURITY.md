# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to the maintainers or through GitHub's private vulnerability reporting feature:

1. Go to the repository's Security tab
2. Click "Report a vulnerability"
3. Fill out the vulnerability report form

### What to Include

When reporting a vulnerability, please include:

- A description of the vulnerability
- Steps to reproduce the issue
- Potential impact of the vulnerability
- Any suggested fixes (if applicable)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your report within 48 hours
- **Initial Assessment**: We will provide an initial assessment within 7 days
- **Resolution Timeline**: We aim to resolve critical vulnerabilities within 30 days
- **Disclosure**: We will coordinate with you on public disclosure timing

### Scope

This security policy covers:

- The `bq-schema-gen` CLI tool
- The `bq_schema_gen` library crate
- Associated build and release infrastructure

### Out of Scope

The following are generally out of scope:

- Issues in dependencies (please report to the respective projects)
- Theoretical vulnerabilities without proof of concept
- Issues requiring physical access to a user's machine
- Social engineering attacks

## Security Considerations

### Input Processing

This tool processes JSON and CSV files from potentially untrusted sources. While we take care to handle malformed input safely:

- The tool does not execute any code from input files
- Memory usage is bounded by input file size
- Invalid input is rejected or skipped (with `--ignore-invalid-lines`)

### File System Access

The tool:

- Only reads files explicitly specified by the user
- Only writes to explicitly specified output paths
- Does not access network resources
- Does not execute external commands (except in watch mode with `--on-change`)

### Watch Mode

When using `--on-change` in watch mode:

- Commands are executed in a shell
- Only use with trusted command strings
- The command receives no input from processed files

## Security Best Practices for Users

1. **Validate input sources**: Only process files from trusted sources
2. **Review output paths**: Ensure output paths don't overwrite sensitive files
3. **Use watch mode carefully**: The `--on-change` flag executes shell commands
4. **Keep updated**: Use the latest version for security fixes

## Acknowledgments

We appreciate the security research community's efforts in improving the security of this project. Contributors who report valid security issues will be acknowledged (with their permission) in release notes.
