# Visual Guide: Parallel vs Sequential Execution

Understanding `zipWith` and `flatMapWith` with diagrams.

---

## 🎯 The Two Core Operators

| Operator | Execution | Use When | RxJava Equivalent |
|----------|-----------|----------|-------------------|
| `zipWith` | **Parallel** | Operations are independent | `Single.zip()` |
| `flatMapWith` | **Sequential** | Operation depends on previous result | `flatMap()` |

---

## ⚡ zipWith (Parallel Execution)

### Visual Flow

```
Start
  │
  ├──────────────┬──────────────┬──────────────┐
  │              │              │              │
  ▼              ▼              ▼              ▼
[Single A]   [Single B]   [Single C]   [Single D]
  │              │              │              │
  │ (all execute simultaneously)│              │
  │              │              │              │
  └──────────────┴──────────────┴──────────────┘
                     │
                     ▼
                  Result
```

### Code Example

```kotlin
::combine.liftSingle()
    .zipWith(fetchUser())     // ⚡ Starts immediately
    .zipWith(fetchConfig())   // ⚡ Starts immediately
    .zipWith(fetchStats())    // ⚡ Starts immediately

// Timeline:
// t=0ms:   All three Singles start executing
// t=200ms: All complete (whichever is slowest)
// Total:   ~200ms (not 600ms!)
```

### Real-World Analogy

Like ordering from **3 food trucks simultaneously**:
- 🌮 Taco truck (200ms)
- 🍕 Pizza truck (150ms)  
- 🍔 Burger truck (180ms)

**Total wait**: 200ms (slowest truck), not 530ms!

---

## ⛓️ flatMapWith (Sequential Execution)

### Visual Flow

```
Start
  │
  ▼
[Single A] ───────┐
  │               │
  │ (waits)       │
  ▼               │
[Single B] ───────┤
  │               │
  │ (waits)       │
  ▼               │
[Single C] ───────┤
  │               │
  │ (waits)       │
  ▼               │
[Single D] ───────┘
  │
  ▼
Result
```

### Code Example

```kotlin
::process.liftSingle()
    .flatMapWith(authenticate())    // 1️⃣ Wait for auth
    .flatMapWith(fetchProfile())    // 2️⃣ Then fetch profile
    .flatMapWith(loadSettings())    // 3️⃣ Then load settings

// Timeline:
// t=0ms:    authenticate() starts
// t=100ms:  authenticate() done, fetchProfile() starts
// t=250ms:  fetchProfile() done, loadSettings() starts  
// t=350ms:  loadSettings() done
// Total:    ~350ms (sum of all)
```

### Real-World Analogy

Like a **recipe with steps**:
1. 🥚 Boil eggs (10 min) → wait
2. 🥗 Chop vegetables (5 min) → wait
3. 🍽️ Mix everything (2 min)

**Total time**: 17 minutes (must do in order!)

---

## 🎨 Mixed Strategy (The Power Move)

Combine both for optimal performance:

### Visual Flow

```
Start
  │
  ▼
[Auth] ────────────────┐ (must complete first)
  │                    │
  ├────────┬───────────┤
  │        │           │
  ▼        ▼           ▼
[API 1] [API 2]   [API 3]  (parallel batch)
  │        │           │
  └────────┴───────────┘
           │
           ▼
      [Payment] ────────┐ (needs previous data)
           │            │
           ▼            │
        Result ─────────┘
```

### Code Example

```kotlin
::checkout.liftSingle()
    .flatMapWith(authenticate())      // 🔒 Must complete first
    .zipWith(fetchCart())             // ⚡ Parallel batch
    .zipWith(checkInventory())        // ⚡ starts
    .zipWith(calculateShipping())     // ⚡ here
    .flatMapWith(processPayment())    // 💳 Needs auth + cart data
    .zipWith(sendEmail())             // ⚡ Fire and forget

// Timeline:
// t=0ms:   authenticate() starts
// t=100ms: auth done → 3 Singles start in parallel
// t=300ms: all 3 done → processPayment() starts
// t=500ms: payment done → sendEmail() starts
// t=600ms: complete
//
// Sequential-only would take: 100+200+200+200+200+100 = 1000ms
// Parallel-only would fail: needs auth before payment!
// Mixed strategy: 600ms ✅
```

---

## 📊 Performance Comparison

### Scenario: Load user dashboard (5 API calls)

#### All Sequential (❌ Slow)

```kotlin
// Total: 1000ms
.flatMapWith(fetchUser())        // 200ms
.flatMapWith(fetchPosts())       // 200ms
.flatMapWith(fetchComments())    // 200ms
.flatMapWith(fetchLikes())       // 200ms
.flatMapWith(fetchFollowers())   // 200ms
```

```
Timeline: ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1000ms
User:     ████────────────────────────────────────
Posts:        ████────────────────────────────────
Comments:         ████────────────────────────────
Likes:                ████────────────────────────
Followers:                ████────────────────────
```

#### All Parallel (✅ Fast)

```kotlin
// Total: 200ms (if all take 200ms)
.zipWith(fetchUser())
.zipWith(fetchPosts())
.zipWith(fetchComments())
.zipWith(fetchLikes())
.zipWith(fetchFollowers())
```

```
Timeline: ████──────────────────────────────────── 200ms
User:     ████────────────────────────────────────
Posts:    ████────────────────────────────────────
Comments: ████────────────────────────────────────
Likes:    ████────────────────────────────────────
Followers:████────────────────────────────────────
```

**Result**: 5x faster! ⚡

---

## 🎯 Decision Tree

```
                    Start
                      │
                      ▼
          Does operation need
          previous result?
                 /     \
               Yes      No
                │       │
                ▼       ▼
         flatMapWith  zipWith
          (Sequential)(Parallel)
```

---

## 💡 Pro Tips

### 1. **Default to parallel**

```kotlin
// If operations are independent, always use zipWith
.zipWith(fetchA())  // ✅ Fastest
.zipWith(fetchB())
.zipWith(fetchC())
```

### 2. **Use sequential when dependent**

```kotlin
// If B needs result of A, use flatMapWith
.flatMapWith(authenticate())  // ✅ Correct
.flatMapWith(fetchProfile())  // Needs auth token
```

### 3. **Batch independent operations**

```kotlin
// ❌ Bad: All sequential
.flatMapWith(fetchA())
.flatMapWith(fetchB())  // Doesn't need A!
.flatMapWith(fetchC())  // Doesn't need A or B!

// ✅ Good: Parallel where possible
.flatMapWith(fetchA())  // Must be first
.zipWith(fetchB())      // Independent of A
.zipWith(fetchC())      // Independent of A and B
```

### 4. **Think in batches**

```kotlin
// Group operations by dependency level
::process.liftSingle()
    // Level 1: Must happen first
    .flatMapWith(validate())
    
    // Level 2: Can happen in parallel (need Level 1)
    .zipWith(fetchData1())
    .zipWith(fetchData2())
    .zipWith(fetchData3())
    
    // Level 3: Needs Level 2 results
    .flatMapWith(aggregate())
    
    // Level 4: Fire and forget
    .zipWith(logEvent())
    .zipWith(sendNotification())
```

---

## 🧪 Test It Yourself

Run this code to see the difference:

```kotlin
fun testParallelVsSequential() {
    fun slowSingle(name: String, delayMs: Long) = 
        Single.timer(delayMs, TimeUnit.MILLISECONDS)
            .map { name }
            .doOnSuccess { println("$it completed at ${System.currentTimeMillis()}") }
    
    // Sequential: ~600ms
    val sequential = ::Pair.liftSingle()
        .flatMapWith(slowSingle("A", 200))
        .flatMapWith(slowSingle("B", 200))
        .flatMapWith(slowSingle("C", 200))
    
    // Parallel: ~200ms
    val parallel = ::Triple.liftSingle()
        .zipWith(slowSingle("A", 200))
        .zipWith(slowSingle("B", 200))
        .zipWith(slowSingle("C", 200))
    
    // Compare execution times
    sequential.blockingGet() // Takes 600ms
    parallel.blockingGet()   // Takes 200ms
}
```

---

## 📚 Summary

| Concept | Key Point | When to Use |
|---------|-----------|-------------|
| **zipWith** | All Singles execute at the same time | Operations are independent |
| **flatMapWith** | Each Single waits for the previous | Operation depends on previous result |
| **Mixed** | Combine both for optimal flow | Real-world scenarios (most common) |

**Golden Rule**: Use parallel (`zipWith`) unless you have a dependency, then use sequential (`flatMapWith`).

---

[← Back to README](README.md) | [See Examples →](EXAMPLES.md)
