# Contributing to KafkaLite

Thank you for your interest in contributing to **KafkaLite**! We welcome bug fixes, documentation patches, and cleanups.

---

## 🛠️ Dev Environment Setup

1. **Prerequisites**: Install [Go 1.21+](https://go.dev/doc/install) and [Docker](https://docs.docker.com/get-docker/).
2. **Clone Repository**:
   ```bash
   git clone https://github.com/yashn035/Kafkalite.git
   cd Kafkalite
   ```

---

## 🧪 Running Tests & Checks

Before submitting any code changes, ensure all tests compile and pass:

```bash
# Run Go unit test suite with race checker
make test

# Run Go static analysis code linter
make lint

# Run local Docker integration failover demo
make demo
```

---

## 📝 Pull Request Guidelines

1. **Keep Changes Focused**: Separate unrelated changes into different branches and PRs.
2. **Follow Coding Conventions**:
   * Keep functions under **30 lines** of code where possible.
   * Write thread-safe abstractions using `sync.RWMutex`.
   * Ensure Go code is formatted using `go fmt ./...`.
3. **No Generics**: To keep the binary small and compilation speeds high, avoid using Go generics.
4. **Document Exports**: Add GoDoc comments to any new exported functions or structures.
