# Contributing

Thanks for your interest in rxjava-applicative-dsl.

## Setup

```bash
git clone https://github.com/damian-rafael-lattenero/rxjava-applicative-dsl
cd rxjava-applicative-dsl
./gradlew test
```

The Gradle wrapper is included. Builds with JDK 21 (Gradle toolchain
resolves it automatically if missing).

## Guidelines

- The public API is intentionally small: `zipWith`, `flatMapWith`, `liftSingle`,
  Kleisli composition, and the Observable/Flowable stream combinators. Proposals
  that grow the surface need a strong rationale.
- New behavior needs tests. Keep fixtures deterministic.
- No dependencies beyond RxJava 3.
- For greenfield coroutine projects, look at
  [KAP](https://github.com/damian-rafael-lattenero/kap) — the successor of this
  DSL. Contributions there are welcome too.

## Pull requests

1. Fork, create a branch, commit with clear messages.
2. Make sure `./gradlew test` is green.
3. Open a PR describing the motivation, not just the diff.
