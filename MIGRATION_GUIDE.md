# Migration Guide: Vanilla RxJava → DSL

Step-by-step guide to refactor your existing RxJava code using the Applicative DSL.

---

## 🎯 Quick Reference

| Vanilla RxJava | DSL Equivalent | Notes |
|----------------|----------------|-------|
| `Single.zip(a, b) { x, y -> f(x, y) }` | `::f.liftSingle().zipWith(a).zipWith(b)` | Parallel execution |
| `a.flatMap { x -> b.map { y -> f(x, y) } }` | `::f.liftSingle().flatMapWith(a).flatMapWith(b)` | Sequential execution |
| `Single.just(value)` | `value.liftSingle()` | Lift value into Single |
| `a.flatMap(::f)` | Use Kleisli: `f.runK(a)` | Kleisli arrow |

---

## 📋 Migration Patterns

### Pattern 1: Simple zip

#### Before

```kotlin
fun combineData(userId: String): Single<Result> {
    return Single.zip(
        fetchUser(userId),
        fetchPosts(userId),
        BiFunction<User, Posts, Result> { user, posts ->
            Result(user, posts)
        }
    )
}
```

#### After

```kotlin
fun combineData(userId: String): Single<Result> {
    return ::Result.liftSingle()
        .zipWith(fetchUser(userId))
        .zipWith(fetchPosts(userId))
}
```

**Benefits**: 
- ✅ No BiFunction boilerplate
- ✅ Type inference works better
- ✅ Easier to add more parameters

---

### Pattern 2: Nested flatMap

#### Before

```kotlin
fun loadProfile(userId: String): Single<Profile> {
    return authenticate(userId).flatMap { token ->
        fetchUserData(token).flatMap { data ->
            enrichProfile(data).map { enriched ->
                Profile(token, data, enriched)
            }
        }
    }
}
```

#### After

```kotlin
fun loadProfile(userId: String): Single<Profile> {
    return ::Profile.liftSingle()
        .flatMapWith(authenticate(userId))
        .flatMapWith(fetchUserData())
        .flatMapWith(enrichProfile())
}
```

**Benefits**:
- ✅ No nested blocks
- ✅ Linear, readable flow
- ✅ Easy to reorder steps

---

### Pattern 3: Mixed zip and flatMap

#### Before

```kotlin
fun checkout(cart: Cart): Single<Order> {
    return validateCart(cart).flatMap { isValid ->
        if (!isValid) {
            Single.error(InvalidCartException())
        } else {
            Single.zip(
                calculateTotal(cart),
                applyDiscounts(cart),
                BiFunction<Double, Double, Double> { total, discount -> 
                    total - discount 
                }
            ).flatMap { finalAmount ->
                processPayment(finalAmount).flatMap { payment ->
                    Single.zip(
                        reserveItems(cart),
                        sendEmail(payment),
                        BiFunction<Reservation, Email, Order> { res, email ->
                            Order(payment, res, email)
                        }
                    )
                }
            }
        }
    }
}
```

#### After

```kotlin
fun checkout(cart: Cart): Single<Order> {
    return ::Order.liftSingle()
        .flatMapWith(validateCart(cart))
        .zipWith(calculateTotal(cart))
        .zipWith(applyDiscounts(cart))
        .flatMapWith(processPayment())
        .zipWith(reserveItems(cart))
        .zipWith(sendEmail())
}
```

**Benefits**:
- ✅ 70% less code
- ✅ Intent is crystal clear
- ✅ Much easier to modify

---

### Pattern 4: Many parameters (5+)

#### Before

```kotlin
fun createReport(): Single<Report> {
    return Single.zip(
        fetchSales(),
        fetchExpenses(),
        BiFunction { sales, expenses -> Pair(sales, expenses) }
    ).flatMap { (sales, expenses) ->
        Single.zip(
            Single.just(sales),
            Single.just(expenses),
            fetchRevenue(),
            Function3 { s, e, revenue -> Triple(s, e, revenue) }
        ).flatMap { (s, e, revenue) ->
            Single.zip(
                Single.just(s),
                Single.just(e),
                Single.just(revenue),
                fetchCosts(),
                fetchProfit(),
                Function5 { sales, exp, rev, costs, profit ->
                    Report(sales, exp, rev, costs, profit)
                }
            )
        }
    }
}
```

#### After

```kotlin
fun createReport(): Single<Report> {
    return ::Report.liftSingle()
        .zipWith(fetchSales())
        .zipWith(fetchExpenses())
        .zipWith(fetchRevenue())
        .zipWith(fetchCosts())
        .zipWith(fetchProfit())
}
```

**Benefits**:
- ✅ 85% less code!
- ✅ No manual carrying of intermediate values
- ✅ Scales to 22 parameters

---

## 🔄 Step-by-Step Migration

### Step 1: Identify the pattern

Look for these code smells:
- Multiple nested `flatMap` calls
- `Single.zip` with `BiFunction`, `Function3`, etc.
- Intermediate `Pair`/`Triple` just to carry values forward
- Deep nesting (3+ levels)

### Step 2: Extract the target function

```kotlin
// Before
Single.zip(a, b, c) { x, y, z -> SomeClass(x, y, z) }

// Identify: SomeClass constructor is the target
// Extract: ::SomeClass
```

### Step 3: Replace with DSL

```kotlin
// After
::SomeClass.liftSingle()
    .zipWith(a)
    .zipWith(b)
    .zipWith(c)
```

### Step 4: Choose operator

- If operations are independent → `zipWith`
- If operation needs previous result → `flatMapWith`

---

## 🎓 Advanced: Kleisli Migration

### Pattern: Composing reactive functions

#### Before

```kotlin
fun validateAndProcess(userId: String): Single<Result> {
    return validateUser(userId)
        .flatMap { user -> fetchOrders(user.id) }
        .flatMap { orders -> processOrders(orders) }
        .flatMap { processed -> sendNotification(processed) }
}
```

#### After (Kleisli style)

```kotlin
val pipeline: Kleisli<String, Result> = 
    ::validateUser
        .andThenK(::fetchOrders)
        .andThenK(::processOrders)
        .andThenK(::sendNotification)

fun validateAndProcess(userId: String): Single<Result> {
    return pipeline.runK(userId)
}
```

**Benefits**:
- ✅ Reusable pipeline
- ✅ Composable building blocks
- ✅ Easier to test each step

---

## 📝 Common Mistakes

### ❌ Mistake 1: Using flatMapWith when zipWith would work

```kotlin
// ❌ Bad: Sequential when parallel would work
::combine.liftSingle()
    .flatMapWith(fetchA())  // Doesn't need previous result
    .flatMapWith(fetchB())  // Doesn't need A!
    .flatMapWith(fetchC())  // Doesn't need A or B!

// ✅ Good: Parallel execution
::combine.liftSingle()
    .zipWith(fetchA())
    .zipWith(fetchB())
    .zipWith(fetchC())
```

**Impact**: 3x slower execution time!

---

### ❌ Mistake 2: Wrong order of parameters

```kotlin
// ❌ Bad: Order matters!
::createUser.liftSingle()  // (name, email, age)
    .zipWith(fetchEmail())
    .zipWith(fetchName())   // Switched order!
    .zipWith(fetchAge())

// ✅ Good: Match function signature
::createUser.liftSingle()  // (name, email, age)
    .zipWith(fetchName())   // 1st param
    .zipWith(fetchEmail())  // 2nd param
    .zipWith(fetchAge())    // 3rd param
```

---

### ❌ Mistake 3: Forgetting to lift

```kotlin
// ❌ Bad: Direct function call
::processData
    .zipWith(fetchA())  // Won't compile!

// ✅ Good: Lift first
::processData.liftSingle()
    .zipWith(fetchA())  // Works!
```

---

## 🧪 Testing During Migration

Use this pattern to test equivalence:

```kotlin
@Test
fun testMigration() {
    val userId = "123"
    
    // Old implementation
    val oldResult = oldVanillaImplementation(userId)
        .blockingGet()
    
    // New DSL implementation
    val newResult = newDSLImplementation(userId)
        .blockingGet()
    
    // They should produce same result
    assertEquals(oldResult, newResult)
}
```

---

## 📊 Migration Checklist

- [ ] Identify all `Single.zip` with 3+ parameters
- [ ] Identify nested `flatMap` chains (3+ levels)
- [ ] Start with lowest complexity functions
- [ ] Add tests before refactoring
- [ ] Refactor one function at a time
- [ ] Run full test suite after each change
- [ ] Update team documentation
- [ ] Code review with team

---

## 💡 Tips for Success

### 1. **Start small**

Begin with simple 2-3 parameter functions before tackling complex ones.

### 2. **Keep old code temporarily**

```kotlin
@Deprecated("Use newImplementation", ReplaceWith("newImplementation()"))
fun oldImplementation() = ...

fun newImplementation() = ...  // New DSL version
```

### 3. **Educate the team**

Share the [Visual Guide](VISUAL_GUIDE.md) and [Examples](EXAMPLES.md) with your team.

### 4. **Measure impact**

Track:
- Lines of code reduced
- Cyclomatic complexity
- Developer feedback

---

## 🚀 Next Steps

1. Read the [Visual Guide](VISUAL_GUIDE.md) to understand operators
2. Try [Examples](EXAMPLES.md) to see real use cases
3. Migrate one file at a time
4. Share wins with the team!

---

## 🆘 Need Help?

- [Open an issue](https://github.com/damian-rafael-lattenero/rxjava-applicative-dsl/issues)
- [Discussion forum](https://github.com/damian-rafael-lattenero/rxjava-applicative-dsl/discussions)

---

[← Back to README](README.md)
