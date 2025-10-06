# Contributing to FLUXNET Shuttle Library

Thank you for your interest in contributing!

## How to Contribute

### For Contributors with Repository Access

If you have push access to the repository (internal contributors, collaborators):

1. Create a feature branch directly in the main repository:
   ```bash
   git checkout main
   git pull origin main
   git checkout -b feature/your-feature-name
   ```
2. Set up your development environment (see below).
3. Make your changes and ensure they pass all checks.
4. Add or update documentation as needed.
5. Push your branch and submit a pull request with a clear description of your changes.

### For External Contributors

If you don't have push access to the repository:

1. Fork the repository and create your branch from `main`.
2. Set up your development environment (see below).
3. Make your changes and ensure they pass all checks.
4. Add or update documentation as needed.
5. Submit a pull request with a clear description of your changes.

**When to fork vs. create a branch directly:**
- **Create a branch directly** if you have repository access - this simplifies collaboration and CI workflows
- **Fork the repository** if you're an external contributor without push access

## Setting Up Your Development Environment

### Python Environment Setup

You can use your preferred Python dependency manager. Here are some options:

#### Using pip (with venv)
```sh
python3.13 -m venv .venv
source .venv/bin/activate
pip install -e .[dev,docs]
```

#### Using uv
```sh
uv venv .venv
source .venv/bin/activate
uv pip install -e .[dev,docs]
```

#### Using poetry
```sh
poetry install --with dev,docs
poetry shell
```

#### Using conda (with environment.yml)
```sh
conda env create -f environment.yml
conda activate fluxnet-shuttle-lib
```

Or, to create the environment manually:
```sh
conda create -n fluxnet-shuttle-lib python=3.13
conda activate fluxnet-shuttle-lib
pip install -e .[dev,docs]
```

## Development Workflow

### Code Quality Checks

Before submitting a pull request, ensure your code passes all quality checks:

```bash
# Format code with black
black .

# Sort imports with isort
isort .

# Lint with flake8
flake8 src/fluxnet_shuttle_lib tests

# Type check with mypy
mypy src/fluxnet_shuttle_lib

# Run tests with coverage
pytest --cov=fluxnet_shuttle_lib --cov-report=term-missing
```

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=fluxnet_shuttle_lib

# Run specific test file
pytest tests/test_core.py

# Run tests with verbose output
pytest -v
```

### Building Documentation

```bash
cd docs
make html
```

The built documentation will be available in `docs/_build/html/`.

### Pre-commit Hooks (Optional)

You can set up pre-commit hooks to automatically run code quality checks:

```bash
pip install pre-commit
pre-commit install
```

## Project Structure

```
fluxnet-shuttle-lib/
├── src/
│   └── fluxnet_shuttle_lib/
│       ├── __init__.py
│       └── core.py
├── tests/
│   └── test_core.py
├── docs/
│   ├── conf.py
│   ├── index.rst
│   └── Makefile
├── pyproject.toml
├── README.md
├── CONTRIBUTING.md
├── LICENSE
└── mypy.ini
```

## Code Standards

- Follow PEP 8 style guidelines (enforced by black and flake8)
- Use type hints for all public functions and methods
- Write comprehensive docstrings for all public APIs
- Maintain test coverage above 90%
- Update documentation when adding new features

## Testing Guidelines

- Write unit tests for all new functionality
- Use descriptive test names that explain what is being tested
- Include edge cases and error conditions in tests
- Mock external dependencies when appropriate

## Documentation Guidelines

- Write clear, comprehensive docstrings for all public APIs
- Use Google-style docstrings for consistency
- Include examples in docstrings when helpful
- Update the main documentation when adding significant features

## Release Process

1. Ensure all tests pass and coverage is adequate
2. Update version number (handled automatically by git tags)
3. Create a pull request with your changes
4. After review and approval, merge to main
5. Tag the release with semantic versioning (e.g., `v1.0.0`)

## Getting Help

If you have questions or need help:
- Open an issue for bugs or feature requests
- Start a discussion for general questions
- Reach out to the maintainers for urgent matters

Thank you for contributing to FLUXNET Shuttle Library!