.PHONY: lint format format-check mypy test check build clean release install-local

# Run ruff linter
lint:
	uv run --with ruff ruff check .

# Run ruff formatter (fix and format)
format:
	uv run --with ruff ruff check --fix .
	uv run --with ruff ruff format .

# Check formatting without making changes
format-check:
	uv run --with ruff ruff format --check .

# Run mypy type checker
mypy:
	uv run --with mypy --with types-PyYAML --with pytest-stub mypy .

# Run tests
test:
	uv run --with pytest pytest

# Run all checks
check: lint format-check mypy

# Packaging and distribution
build:
	@echo "Building package..."
	uv build

clean:
	@echo "Cleaning build artifacts..."
	rm -rf dist/ build/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

release: check test clean build
	@echo "All checks passed! Creating GitHub release..."
	@VERSION=$$(grep -m 1 'version = ' pyproject.toml | cut -d '"' -f 2); \
	echo "Version: $$VERSION"; \
	gh release create "v$$VERSION" dist/*.whl dist/*.tar.gz \
		--title "Release v$$VERSION" \
		--generate-notes

install-local: clean build
	@echo "Installing locally with uv tool..."
	uv tool install --force dist/*.whl
