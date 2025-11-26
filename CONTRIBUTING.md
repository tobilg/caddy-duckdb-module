# Contributing to Caddy DuckDB Extension

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Code of Conduct

Please be respectful and constructive in all interactions. We're all here to build something useful together.

## How to Contribute

### Reporting Bugs

Before submitting a bug report:

1. Check existing issues to avoid duplicates
2. Use the latest version of the project
3. Collect relevant information:
   - Go version (`go version`)
   - Operating system and architecture
   - Steps to reproduce the issue
   - Expected vs actual behavior
   - Error messages and logs

When opening an issue, include:
- A clear, descriptive title
- Detailed description of the problem
- Minimal reproduction steps
- Any relevant configuration

### Suggesting Features

Feature requests are welcome! Please:

1. Check existing issues for similar requests
2. Describe the use case and problem you're solving
3. Explain your proposed solution
4. Consider alternatives you've thought about

### Submitting Pull Requests

#### Before You Start

1. Open an issue to discuss significant changes
2. For small fixes, you can submit a PR directly
3. Ensure your change aligns with the project's goals

#### Development Setup

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed setup instructions.

```bash
# Quick setup
git clone https://github.com/tobilg/caddyserver-duckdb-module.git
cd caddyserver-duckdb-module
make setup
```

#### Making Changes

1. **Fork the repository** and create your branch:
   ```bash
   git checkout -b feature/my-feature
   # or
   git checkout -b fix/my-bugfix
   ```

2. **Make your changes**:
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**:
   ```bash
   make lint    # Format and vet
   make test    # Run tests
   make run     # Manual testing
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```

   Follow [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` - New feature
   - `fix:` - Bug fix
   - `docs:` - Documentation only
   - `refactor:` - Code change that neither fixes a bug nor adds a feature
   - `test:` - Adding or updating tests
   - `chore:` - Maintenance tasks

5. **Push and create a Pull Request**:
   ```bash
   git push origin feature/my-feature
   ```

#### Pull Request Guidelines

- **Title**: Use a clear, descriptive title
- **Description**: Explain what the PR does and why
- **Tests**: Include tests for new functionality
- **Documentation**: Update relevant documentation
- **Size**: Keep PRs focused and reasonably sized

Example PR description:
```markdown
## Summary
Add CSV export support for query results.

## Changes
- Add `formats/csv.go` with CSV serialization
- Update content negotiation in handlers
- Add tests for CSV output

## Testing
- Added unit tests for CSV formatter
- Manually tested with curl: `curl -H "Accept: text/csv" ...`

## Related Issues
Fixes #42
```

## Style Guide

### Go Code

- Follow standard Go conventions
- Use `gofmt` for formatting (automatic with `make fmt`)
- Run `go vet` for static analysis (automatic with `make vet`)
- Keep functions focused and reasonably sized
- Add comments for exported functions and types
- Use meaningful variable and function names

### Error Handling

- Always handle errors explicitly
- Provide context in error messages
- Use structured logging with `zap`

```go
// Good
if err != nil {
    d.logger.Error("failed to execute query",
        zap.String("query", sql),
        zap.Error(err),
    )
    return fmt.Errorf("execute query: %w", err)
}

// Avoid
if err != nil {
    return err
}
```

### Testing

- Write tests for new functionality
- Use table-driven tests where appropriate
- Test edge cases and error conditions
- Keep tests focused and readable

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
        wantErr  bool
    }{
        {"valid input", "foo", "FOO", false},
        {"empty input", "", "", false},
        {"invalid input", "!", "", true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := MyFunction(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("unexpected error: %v", err)
            }
            if result != tt.expected {
                t.Errorf("got %q, want %q", result, tt.expected)
            }
        })
    }
}
```

### Documentation

- Update README.md for user-facing changes
- Update DEVELOPMENT.md for developer-facing changes
- Add inline comments for complex logic
- Update OpenAPI spec for API changes

## Review Process

1. All PRs require at least one review
2. CI must pass (tests, linting)
3. Address review feedback
4. Squash commits if requested

## Questions?

If you have questions:
- Check existing documentation
- Search existing issues
- Open a discussion or issue

Thank you for contributing!
