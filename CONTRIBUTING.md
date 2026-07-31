# Contributing to KafkaLite

Welcome to the KafkaLite project! We'd love your help to make it even better.

## 🛠️ Development Setup

1. **Prerequisites**: Go 1.21+, Docker, Make.
2. **Clone the repo**: `git clone https://github.com/yashn035/Kafkalite.git`
3. **Build the project**: 
   ```bash
   make build
   ```
4. **Run tests**:
   ```bash
   make test
   ```

## 📝 Coding Standards

To ensure a highly readable and maintainable codebase, we strictly enforce the following rules:
- **Gofmt**: All code must be formatted with `gofmt`. Run `make lint` before submitting a PR.
- **Function Length**: Keep all functions under **30 lines**. If a function grows larger, break it down into smaller, composable helper functions.
- **Concurrency**: Always use `sync.RWMutex` for shared memory and thoroughly test for data races.

## 🔄 Pull Request Workflow

1. Fork the repository.
2. Create a new feature branch (`git checkout -b feature/my-feature`).
3. Commit your changes. Ensure tests pass (`make test`).
4. Push to your branch and open a Pull Request against `main`.
5. A maintainer will review your code. CI will automatically run tests and generate a coverage report.

Thank you for contributing!
