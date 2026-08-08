# Proposal: Coexistence of Structural API and Fluent Generic Method API in beam-go

## Overview & Goal

With Go 1.27 introducing **Generic Methods** (allowing methods on types to introduce new type parameters beyond those declared on the receiver type), `beam-go` can support a **Fluent Generic Method API** alongside its existing **Structural API**.

The primary objective is to enable both styles within a **single package (`package beam`)** and inside the **exact same pipeline**, ensuring 100% backward compatibility while providing cleaner ergonomics for linear data transformations.

---

## Comparative Matrix

| Concept | Structural API (Current) | Fluent Generic Method API (Go 1.27) |
| :--- | :--- | :--- |
| **Primary Receiver** | `s *beam.Scope` (or global package functions) | `p beam.PCol[E]` |
| **Output Definition** | Exported `PCol[T]` fields in DoFn structs | Direct returns (`PCol[O]` or multi-output structs) |
| **Pipeline Style** | Explicit graph node binding & struct access | Method chaining (`.Map().Filter().GBK()`) |
| **Primary Use Cases** | Multi-output DoFns, Side inputs, Stateful/Timer DoFns, SDFs | Single-input/single-output maps, filters, linear transforms |
| **Underlying Mechanism** | Direct `Transform[E]` struct reflection (`deferDoFn`) | Synthetic `mapper[I, O]` wrapping lambdas into `DFC[I]` |

---

## Handling Advanced DoFn Facets & `lightweight.go`

A critical design choice is how to handle advanced DoFn facets (Bundle Lifecycle `StartBundle`/`FinishBundle`, State & Timers, Splittable DoFns) across both APIs.

### 1. Underlying Handler Reuse
Both APIs share the exact same runtime infrastructure:
- The Fluent API **supplants and expands `lightweight.go`**.
- When `p.Map(s, lambda)` is called, it constructs an internal `mapper[I, O]` struct (defined in `lightweight.go`) implementing `ProcessBundle(dfc *DFC[I]) error`.
- Execution, serialization, and worker reconstruction still go through the standard `DFC[E]` pipeline harness.

### 2. Division of Responsibility for Advanced Facets

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FACET & FEATURE DIVISION                            │
├──────────────────────────────────┬──────────────────────────────────────────┤
│ Fluent API (PCol[E] Methods)     │ Structural API (DoFn Structs via Scope)  │
├──────────────────────────────────┼──────────────────────────────────────────┤
│ • Lightweight lambdas & functions│ • Bundle Lifecycle (StartBundle/         │
│   (Map, Filter, FlatMap)         │   FinishBundle)                          │
│ • Keyed aggregations             │ • Keyed State & Timers (State[V],        │
│   (GBK, CombinePerKey)           │   Timer fields via StatefulParDo)        │
│ • PTransform delegation          │ • Splittable DoFns (SDFs & trackers)     │
│   via p.Apply(s, transform)      │ • Multi-output PCol emitters             │
│                                  │ • Side Input access                      │
└──────────────────────────────────┴──────────────────────────────────────────┘
```

---

## Detailed Examples of Each Fluent Method

Below are code examples demonstrating each generic method attached to `PCol[E]`.

### 1. `Map` — 1-to-1 Element Transformation
Transforms each input element into an output element of type `O`.

```go
// Convert strings to uppercase
upperLines := lines.Map(s, func(line string) string {
    return strings.ToUpper(line)
})

// Pair strings with an initial count of 1
paired := words.Map(s, func(w string) beam.KV[string, int] {
    return beam.Pair(w, 1)
})
```

---

### 2. `Filter` — Predicate-Based Element Selection
Retains only elements where the predicate function returns `true`.

```go
// Filter out empty lines
nonEmpty := lines.Filter(s, func(line string) bool {
    return len(strings.TrimSpace(line)) > 0
})

// Keep only words longer than 3 characters
longWords := words.Filter(s, func(word string) bool {
    return len(word) > 3
})
```

---

### 3. `FlatMap` — 1-to-N Element Expansion
Splits a single element into zero, one, or many elements (returning a slice or iterator).

```go
// Tokenize a sentence into individual words
words := lines.FlatMap(s, func(line string) []string {
    return strings.Fields(line)
})
```

---

### 4. `GBK` (GroupByKey) — Keyed Grouping
Groups values by key on a `PCol[KV[K, V]]`, returning `PCol[KV[K, Iter[V]]]`.

```go
// Input: PCol[KV[string, int]]
// Output: PCol[KV[string, Iter[int]]]
grouped := pairedKV.GBK(s)
```

---

### 5. `CombinePerKey` — Keyed Aggregation
Combines grouped values per key using an associative and commutative merger function or `CombineFn`.

```go
// Sum counts per word
wordCounts := pairedKV.CombinePerKey(s, beam.SimpleMerge(sum[int]{}))
```

---

### 6. `Apply` — Applying Composite & Custom Transforms
Attaches a composite transform or sub-pipeline to the current `PCol`.

```go
// Apply a custom CountWords composite transform
counts := lines.Apply(s, &CountWordsTransform{SmallWordLength: 5})

// Apply multi-output validation transform
results := lines.Apply(s, &ValidateAndSplitTransform{})
validLines := results.Valid
```

---

### 7. `ParDo` — Fluent Execution of Single-Output Structural DoFns
Allows running a structural `DoFn` struct (with counters or metrics) fluently when it has a single output.

```go
type normalizeFn struct {
    TrimmedCounter beam.CounterInt64
}

func (fn *normalizeFn) ProcessBundle(dfc *beam.DFC[string]) error {
    return dfc.Process(func(ec beam.ElmC, line string) error {
        fn.TrimmedCounter.Inc(dfc, 1)
        // ...
        return nil
    })
}

// Invoke single-output DoFn fluently
normalized := lines.ParDo(s, &normalizeFn{})
```

---

## Phased Implementation Plan

### Phase 1: API Surface Synthesis & Receiver Partitioning
Rather than creating synthetic holder types (e.g. `beam.Structural`), API methods are cleanly partitioned between two existing core types:

1. **`PCol[E]` (Data-Stream Centric / Fluent API)**:
   - `p.Map(s, fn)`
   - `p.Filter(s, fn)`
   - `p.FlatMap(s, fn)`
   - `p.GBK(s)`
   - `p.CombinePerKey(s, fn)`
   - `p.Apply(s, transform)`
   - `p.ParDo(s, dofn)`
2. **`Scope` (`*Scope`) (Graph-Centric / Structural API)**:
   - `s.ParDo(input, dofn)`
   - `s.StatefulParDo(input, dofn)`
   - `s.Impulse()`
   - `s.Create(elms...)`
   - `s.Expand(name, composite)`

Top-level functions (`beam.ParDo(s, in, dofn)`) remain as aliases delegating to `s.ParDo` or `p.Map`.

### Phase 2: Decoupling Graph Mutation from Syntax
Both APIs compile down to the exact same graph representation (`nodeIndex`, `edgeIndex`, `DFC[E]`):

```go
package beam

// Generic Method on PCol[E] introducing new Output type [O Element]
func (p PCol[E]) Map[O Element](s *Scope, fn func(E) O, opts ...Options) PCol[O] {
    return Map(s, p, fn, opts...)
}

// Generic Method for single-output ParDo on PCol[E]
func (p PCol[E]) ParDo[O Element, DF Transform[E]](s *Scope, dofn DF, opts ...Options) PCol[O] {
    ParDo(s, p, dofn, opts...)
    return extractPrimaryOutput[O](dofn)
}
```

### Phase 3: Multi-Output Interoperability & Apply Pattern
Java Beam relies on `PCollectionTuple` and `TupleTag<T>` because of type erasure. Go uses typed structs and structural returns:

```go
// Generic Transform Interface
type Transform[In, Out any] interface {
    Expand(s *Scope, input In) Out
}

// Method on PCol[In] for applying any Transform
func (p PCol[In]) Apply[Out any](s *Scope, t Transform[PCol[In], Out]) Out {
    return t.Expand(s, p)
}

// Multi-Output Return Example via Go Structs
type SplitOutput struct {
    Valid PCol[string]
    Errors PCol[string]
}

type ValidateDoFn struct{}

func (v *ValidateDoFn) Expand(s *Scope, lines PCol[string]) SplitOutput { ... }

// Usage:
res := lines.Apply(s, &ValidateDoFn{})
// res.Valid and res.Errors are strongly typed PCol[string]
```

---

## When to Use Which Approach

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            RECOMMENDED GUIDELINES                           │
├──────────────────────────────────┬──────────────────────────────────────────┤
│ Use PCol[E] (Fluent API) when:  │ Use Scope / Structural API when:         │
├──────────────────────────────────┼──────────────────────────────────────────┤
│ • Linear 1:1 or 1:N maps         │ • Multi-output DoFns (emitting to        │
│   and filters                    │   multiple PCols via struct fields)      │
│ • Applying self-contained        │ • Stateful / Timed processing            │
│   composite transforms           │ • Splittable DoFns (SDFs)                │
│ • Single-output transformations  │ • DoFns requiring side inputs            │
│ • Concise, readable pipelines    │ • Pipeline sources (Impulse, Read roots) │
└──────────────────────────────────┴──────────────────────────────────────────┘
```

---

## Example: Mixed Coexistence Pipeline

```go
func WordCountPipeline(s *beam.Scope) error {
    // 1. Fluent Read
    lines := textio.Read(s, "gs://apache-beam-samples/", "kinglear.txt")

    // 2. Structural Multi-Output DoFn
    extract := s.ParDo(lines, &extractFn{SmallWordLength: 9})

    // 3. Fluent Chaining from Structural Output
    formatted := extract.Words.
        FlatMap(s, func(line string) []string {
            return strings.Fields(line)
        }).
        Filter(s, func(word string) bool {
            return len(word) > 2
        }).
        Map(s, func(w string) beam.KV[string, int] {
            return beam.Pair(w, 1)
        }).
        CombinePerKey(s, beam.SimpleMerge(sum[int]{})).
        Map(s, func(kv beam.KV[string, int]) string {
            return fmt.Sprintf("%s: %d", kv.Key, kv.Value)
        })

    textio.WriteSingle(s, "file:///tmp/wordcount/", "counts.txt", formatted)
    return nil
}
```
