# RxJava Applicative DSL

> **Type-safe functional composition for RxJava Singles with clean, declarative syntax**

[![Kotlin](https://img.shields.io/badge/Kotlin-2.3+-purple.svg)](https://kotlinlang.org)
[![RxJava](https://img.shields.io/badge/RxJava-3.x-blue.svg)](https://github.com/ReactiveX/RxJava)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **Lineage.** This library is the ancestor of
> [KAP](https://github.com/damian-rafael-lattenero/kap). It was born in 2025
> from production pain — deeply nested `flatMap`/`zip` chains in an RxJava3
> BFF — and its applicative/monadic core later evolved into type-safe
> coroutine orchestration. First published 2025-11-02; repository relocated
> to this account on 2026-10-01 (commit history preserved).

## The Problem

Composing multiple `Single<T>` operations in RxJava quickly becomes verbose and hard to read:

```kotlin
// Vanilla RxJava - Nested, hard to follow
Single.zip(
    fetchUser(),
    fetchConfig(),
    BiFunction<User, Config, Pair<User, Config>> { user, config -> Pair(user, config) }
).flatMap { (user, config) ->
    Single.zip(
        Single.just(user),
        Single.just(config),
        validatePermissions(user),
        Function3<User, Config, Permissions, Result> { u, c, p -> 
            processRequest(u, c, p) 
        }
    )
}
```

## The Solution

Elegant, type-safe composition with clear intent:

```kotlin
// With RxJava Applicative DSL - Clean and declarative
::processRequest.liftSingle()
    .zipWith(fetchUser())           // Execute in parallel
    .zipWith(fetchConfig())         // Execute in parallel
    .flatMapWith(validatePermissions())  // Then execute sequentially
```

**Same result. Far less code. Same type safety.**

---

## ✨ Key Features

- 🎯 **Type-safe** - Full type inference, no runtime surprises
- ⚡ **Parallel & Sequential** - Explicit control over execution strategy
- 🔗 **Kleisli Composition** - Chain reactive functions elegantly
- 📦 **Up to 22 parameters** - Handle complex scenarios
- 🧪 **Tested** — 34 unit tests across 6 suites: error propagation, disposal, 11-parameter compositions, concurrency semantics
- 🚀 **Zero dependencies** - Only RxJava 3.x required

---

## 🚀 Quick Start

### Installation

Not published to Maven Central — this is a source-first library with zero
dependencies beyond RxJava. Two ways to use it:

**Option A: publish to your local Maven** (coordinates
`io.github.damian-rafael-lattenero:rxjava-applicative-dsl:0.1.0`):

```bash
git clone https://github.com/damian-rafael-lattenero/rxjava-applicative-dsl
cd rxjava-applicative-dsl && ./gradlew publishToMavenLocal
```

```gradle
implementation 'io.github.damian-rafael-lattenero:rxjava-applicative-dsl:0.1.0'
```

**Option B: copy the source** — `src/main/kotlin` is 5 small files; drop them
into your project.

### Basic Usage

```kotlin
import utils.liftSingle

// Define your service function
fun createUser(name: String, email: String, age: Int): User {
    return User(name, email, age)
}

// Compose with Singles
val result: Single<User> = ::createUser.liftSingle()
    .zipWith(fetchNameFromAPI())      // Parallel
    .zipWith(fetchEmailFromDB())      // Parallel
    .flatMapWith(calculateAge())      // Sequential
    
result.subscribe { user -> 
    println("Created: $user") 
}
```

---

## 📚 Core Concepts

### 1. Parallel Execution with `zipWith`

Executes Singles **in parallel** and combines results:

```kotlin
// Both API calls execute simultaneously
::combineData.liftSingle()
    .zipWith(fetchFromAPI1())  // ⚡ Parallel
    .zipWith(fetchFromAPI2())  // ⚡ Parallel
    .zipWith(fetchFromAPI3())  // ⚡ Parallel
```

**Use when**: Operations are independent and can run concurrently.

### 2. Sequential Execution with `flatMapWith`

Executes Singles **one after another** (each waits for the previous):

```kotlin
// Executes in order: auth → profile → settings
::loadUserData.liftSingle()
    .flatMapWith(authenticate())        // 1️⃣ First
    .flatMapWith(fetchProfile())        // 2️⃣ Then
    .flatMapWith(loadSettings())        // 3️⃣ Finally
```

**Use when**: Operations depend on previous results.

### 3. Mixed Execution Strategy

Combine both for optimal performance:

```kotlin
::processOrder.liftSingle()
    .flatMapWith(validateUser())        // Sequential: must validate first
    .zipWith(fetchInventory())          // Parallel: independent
    .zipWith(calculateShipping())       // Parallel: independent
    .flatMapWith(processPayment())      // Sequential: needs validated data
    .zipWith(sendConfirmationEmail())   // Parallel: fire and forget
```

---

## 💡 Real-World Example

### Before: Vanilla RxJava

```kotlin
fun createMobileApp(): Single<MobileApp> {
    return fetchAppId().flatMap { id ->
        Single.zip(
            Single.just(id),
            fetchAppName(),
            fetchDescription(),
            BiFunction { i, n, d -> Triple(i, n, d) }
        ).flatMap { (id, name, desc) ->
            Single.zip(
                Single.just(id),
                Single.just(name),
                Single.just(desc),
                fetchVersion(),
                fetchOwner(),
                Function5 { i, n, d, v, o -> /* ... */ }
            ).flatMap { /* 10 more parameters... */ }
        }
    }
}
```

### After: With DSL

```kotlin
fun createMobileApp(): Single<MobileApp> {
    return ::MobileApp.liftSingle()
        .flatMapWith(fetchAppId())          // Must fetch ID first
        .flatMapWith(fetchAppName())        // Then name
        .zipWith(fetchDescription())        // These can run in parallel
        .zipWith(fetchVersion())            // ↓
        .zipWith(fetchOwner())              // ↓
        .zipWith(fetchPlatform())           // ↓
        .flatMapWith(authenticate())        // Then authenticate
        .zipWith(fetchAnalytics())          // Final parallel batch
        .zipWith(fetchPermissions())        // ↓
}
```

**Result**: Clearer intent, easier to modify, same type safety.

---

## 🔗 Kleisli Composition

Chain multiple reactive operations elegantly:

```kotlin
typealias Kleisli<A, B> = (A) -> Single<B>

val pipeline: Kleisli<UserId, OrderResult> = 
    validateUser 
        .andThenK(fetchOrders)
        .andThenK(processPayment)
        .andThenK(sendConfirmation)

// Execute the pipeline
pipeline.runK(userId).subscribe { result -> 
    println("Order completed: $result")
}
```

---

## 📖 API Reference

### Core Functions

| Function | Description | Execution |
|----------|-------------|-----------|
| `liftSingle()` | Lift a function into Single context | - |
| `zipWith(single)` | Apply function with parallel execution | Parallel |
| `flatMapWith(single)` | Apply function with sequential execution | Sequential |
| `andThenK(f)` | Compose Kleisli arrows | Sequential |
| `runK(input)` | Execute Kleisli with input | - |

### Stream Processing (Observable/Flowable)

| Function | Description | Behavior |
|----------|-------------|----------|
| `combine(stream)` | combineLatest - re-evaluate on any emission | Reactive |
| `pair(stream)` | zip - wait for both to emit | Synchronized |
| `chain(stream)` | flatMap - sequential processing | Sequential |

---

## 🎯 When to Use This

### ✅ Great for:

- Legacy projects using RxJava
- Complex multi-step reactive workflows
- Teams familiar with functional programming
- Scenarios with 5+ Singles to compose

### ⚠️ Consider alternatives:

- **New projects**: Use Kotlin Coroutines with `async/await`
- **Simple cases**: Vanilla `Single.zip` might be sufficient
- **Teams unfamiliar with FP**: May have learning curve

---

## 🧪 Testing

Run the comprehensive test suite:

```bash
./gradlew test
```

Includes:
- ✅ 11-parameter complex compositions
- ✅ Error handling and disposal
- ✅ Concurrent vs sequential execution verification
- ✅ Real-world scenarios (trading risk assessment)---

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

1. **More examples** - Real-world use cases
2. **Coroutines version** - Modern Kotlin alternative
3. **Performance benchmarks** - vs vanilla RxJava
4. **Better naming** - Open to suggestions on operator names

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Inspired by functional programming patterns from Haskell, Scala, and Arrow-kt.

Built with ❤️ for the RxJava community.

---

## 📬 Contact

- **Issues**: [GitHub Issues](https://github.com/damian-rafael-lattenero/rxjava-applicative-dsl/issues)
- **Discussions**: [GitHub Discussions](https://github.com/damian-rafael-lattenero/rxjava-applicative-dsl/discussions)

---

⭐ **Star this repo** if you find it useful!
