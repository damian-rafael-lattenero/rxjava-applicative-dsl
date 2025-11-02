# RxJava Applicative DSL

> **Type-safe functional composition for RxJava Singles with clean, declarative syntax**

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9+-purple.svg)](https://kotlinlang.org)
[![RxJava](https://img.shields.io/badge/RxJava-3.x-blue.svg)](https://github.com/ReactiveX/RxJava)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

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

**Same result. 70% less code. 100% more readable.**

---

## ✨ Key Features

- 🎯 **Type-safe** - Full type inference, no runtime surprises
- ⚡ **Parallel & Sequential** - Explicit control over execution strategy
- 🔗 **Kleisli Composition** - Chain reactive functions elegantly
- 📦 **Up to 22 parameters** - Handle complex scenarios
- 🧪 **Battle-tested** - Comprehensive test suite with real-world examples
- 🚀 **Zero dependencies** - Only RxJava 3.x required

---

## 🚀 Quick Start

### Installation

```gradle
// Coming soon to Maven Central
implementation 'com.github.developer-hatch:rxjava-applicative-dsl:1.0.0'
```

### Basic Usage

```kotlin
import liftSingle
import concurrent  // parallel execution
import sequential  // sequential execution

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
fun createMovileApp(): Single<MovileApp> {
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
fun createMovileApp(): Single<MovileApp> {
    return ::MovileApp.liftSingle()
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
- ✅ Real-world scenarios (trading risk assessment)

---

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

- **Issues**: [GitHub Issues](https://github.com/developer-hatch/rxjava-applicative-dsl/issues)
- **Discussions**: [GitHub Discussions](https://github.com/developer-hatch/rxjava-applicative-dsl/discussions)

---

⭐ **Star this repo** if you find it useful!
