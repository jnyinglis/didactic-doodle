# LINQ.js Operator Tutorial

This tutorial demonstrates every operator exposed by `linq.js`. Each section groups related methods and shows short runnable
examples. All snippets assume the ES module import:

```js
import Enumerable from '../linq.js'
```

Feel free to run the snippets in `node` (remember to enable ES modules) or paste them in a REPL.

## Sequence Creation Helpers

### `Enumerable.choice`
Pick a random element from the supplied arguments each time the sequence advances.

```js
Enumerable.choice('heads', 'tails').take(5).toArray()
// => e.g. ['tails', 'tails', 'heads', 'tails', 'heads']
```

### `Enumerable.cycle`
Loop through the supplied arguments forever.

```js
Enumerable.cycle('A', 'B', 'C').take(7).toArray()
// => ['A', 'B', 'C', 'A', 'B', 'C', 'A']
```

### `Enumerable.empty`
Create an empty sequence.

```js
Enumerable.empty().isEmpty()
// => true
```

### `Enumerable.from`
Convert arrays, iterables, array-like values, or plain objects into a LINQ sequence.

```js
// Arrays become enumerable immediately
Enumerable.from(['Ada', 'Grace', 'Lin']).select(name => name.toUpperCase()).toArray()
// => ['ADA', 'GRACE', 'LIN']

// Objects are exposed as `{ key, value }` pairs
Enumerable.from({ name: 'linq', version: 4 })
  .select(entry => `${entry.key}: ${entry.value}`)
  .toArray()
// => ['name: linq', 'version: 4']
```

### `Enumerable.make`
Create a single-element sequence.

```js
Enumerable.make(42).toArray()
// => [42]
```

### `Enumerable.matches`
Enumerate regular-expression matches as rich `RegExpMatchArray` objects. Each emitted match contains the full match, capture
groups, and metadata such as the index where the match started. This is handy when you want to perform incremental parsing or
drive further projections based on the structure of a string.

```js
// Walk a Markdown list and extract both the bullet marker and the text that follows it.
Enumerable.matches('- Learn\n* Build\n- Share', /(?<bullet>[-*])\s+(?<text>.+)/g)
  .select(match => `${match.groups.bullet === '-' ? 'Task' : 'Idea'}: ${match.groups.text}`)
  .toArray()
// => ['Task: Learn', 'Idea: Build', 'Task: Share']
```

Unlike `String.prototype.matchAll`, `Enumerable.matches` integrates directly with the rest of the LINQ operators so you can
filter, group, or transform matches without leaving the fluent chain.

### `Enumerable.range`
Generate numbers starting from `start` for `count` steps.

```js
Enumerable.range(1, 5).toArray()
// => [1, 2, 3, 4, 5]
```

### `Enumerable.rangeDown`
Count down from `start` for `count` steps.

```js
Enumerable.rangeDown(5, 5).toArray()
// => [5, 4, 3, 2, 1]
```

### `Enumerable.rangeTo`
Walk from `start` toward `to` (inclusive) using an optional step.

```js
Enumerable.rangeTo(2, 10, 3).toArray()
// => [2, 5, 8]
```

### `Enumerable.repeat`
Repeat a value a specific number of times (or indefinitely when `count` is omitted).

```js
Enumerable.repeat('na', 4).concat(Enumerable.make('Batman!')).toArray()
// => ['na', 'na', 'na', 'na', 'Batman!']
```

### `Enumerable.repeatWithFinalize`
Construct values on demand and guarantee that accompanying teardown logic runs even if the consumer stops early. Reach for
`repeatWithFinalize` when the generated value needs a matching cleanup step such as disposing a resource, returning a pooled
object, or logging lifecycle events.

```js
const auditTrail = []

const getConnections = Enumerable.repeatWithFinalize(
  () => {
    const id = `conn-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
    auditTrail.push(`open:${id}`)
    return { id }
  },
  connection => auditTrail.push(`close:${connection.id}`)
)

getConnections
  .take(2)
  .select(conn => conn.id)
  .toArray()
// => ['conn-…', 'conn-…'] and auditTrail alternates between matching open/close entries
```

Because the finalizer runs for each yielded value, pooled or borrowed resources are always released.

### `Enumerable.generate`
Call a factory on every iteration to produce values that depend on the latest state. Use it for synthetic data, timestamps,
or any scenario where you want a fresh computation each time rather than a shared reference.

```js
let counter = 0

Enumerable.generate(() => ({ id: ++counter, createdAt: new Date().toISOString() }), 2)
  .select(ticket => `${ticket.id}@${ticket.createdAt}`)
  .toArray()
// => ['1@2024-03-25T…', '2@2024-03-25T…'] (timestamps differ for each element)
```

The sequence stops automatically when `count` is supplied; omit it to keep generating values until the consumer stops.

### `Enumerable.toInfinity`
An infinite increasing sequence.

```js
Enumerable.toInfinity(10, 5).take(4).toArray()
// => [10, 15, 20, 25]
```

### `Enumerable.toNegativeInfinity`
An infinite decreasing sequence.

```js
Enumerable.toNegativeInfinity(-5, 2).take(4).toArray()
// => [-5, -7, -9, -11]
```

### `Enumerable.unfold`
Generate elements by repeatedly applying a function to the prior value.

```js
Enumerable.unfold({ value: 1 }, state => ({ value: state.value * 2 }))
  .select(x => x.value)
  .take(4)
  .toArray()
// => [1, 2, 4, 8]
```

### `Enumerable.defer`
Delay sequence creation until iteration begins. This is ideal for referencing mutable state or expensive operations that
should run anew for each consumer.

```js
let refreshCount = 0
let latestConfig = { featureEnabled: false }

const configSnapshots = Enumerable.defer(() => {
  refreshCount++
  return Enumerable.from(Object.entries(latestConfig))
})

latestConfig = { featureEnabled: true, locale: 'en-US' }
configSnapshots.select(entry => entry.join(',')).toArray()
// => ['featureEnabled,true', 'locale,en-US']; refreshCount == 1

latestConfig = { featureEnabled: false }
configSnapshots.select(entry => entry.join(',')).toArray()
// => ['featureEnabled,false']; refreshCount == 2
```

Every iteration gets a fresh enumerable that reflects the newest state.

## Traversal and Projection

### `traverseBreadthFirst`
Walk a tree layer by layer.

```js
const tree = { value: 'root', children: [{ value: 'a', children: [] }, { value: 'b', children: [] }] }
Enumerable.from([tree])
  .traverseBreadthFirst(node => Enumerable.from(node.children), (node, depth) => `${' '.repeat(depth)}${node.value}`)
  .toArray()
// => ['root', ' a', ' b']
```

### `traverseDepthFirst`
Depth-first traversal with optional projection.

```js
Enumerable.from([tree])
  .traverseDepthFirst(node => Enumerable.from(node.children), (node, depth) => `${depth}:${node.value}`)
  .toArray()
// => ['0:root', '1:a', '1:b']
```

### `flatten`
Remove one level of nesting.

```js
Enumerable.from([[1, 2], [3]]).flatten().toArray()
// => [1, 2, 3]
```

### `pairwise`
Apply a selector to consecutive pairs. Because each element is paired with its predecessor, `pairwise` shines when you need
to calculate deltas or highlight transitions in a sequence.

```js
// Spot stock price changes day-over-day.
const closingPrices = [101, 103, 99, 102]
Enumerable.from(closingPrices)
  .pairwise((previous, current) => ({ previous, current, delta: current - previous }))
  .toArray()
// => [
//      { previous: 101, current: 103, delta: 2 },
//      { previous: 103, current: 99, delta: -4 },
//      { previous: 99, current: 102, delta: 3 }
//    ]
```

When you want to compare an element with either its previous or next neighbor but need more control over indexing, you can
also emulate the same idea with `zip` by pairing the sequence with a shifted copy:

```js
const readings = [15, 14, 16, 18]
// Observe neighbors by zipping against shifted copies of the same list.
const withNext = Enumerable.from(readings)
  .zip(Enumerable.from(readings).skip(1), (current, next) => ({ current, next }))
  .toArray()
// => [
//      { current: 15, next: 14 },
//      { current: 14, next: 16 },
//      { current: 16, next: 18 }
//    ]

const withPrevious = Enumerable.from(readings)
  .skip(1)
  .zip(readings, (current, previous) => ({ previous, current }))
  .toArray()
// => [
//      { previous: 15, current: 14 },
//      { previous: 14, current: 16 },
//      { previous: 16, current: 18 }
//    ]
```

### `scan`
Emit running accumulations. Works with implicit and explicit seeds. `scan` is a great fit for situations where you want to
surface intermediate results such as running totals, moving averages, or the evolving state of a reducer function.

```js
// Running total from a stream of cash register transactions.
const transactions = [5, -2, 13, -4]
Enumerable.from(transactions)
  .scan((total, amount) => total + amount)
  .toArray()
// => [5, 3, 16, 12]

// Provide an explicit seed to ensure the balance starts from zero even if the sequence is empty.
Enumerable.from(transactions)
  .scan(0, (total, amount) => total + amount)
  .toArray()
// => [5, 3, 16, 12]
```

`scan` also combines nicely with `zip` when you need to compare the running total with the original values:

```js
// Pair each purchase with the balance that follows it.
const purchases = [12, 7, 5]
const balances = Enumerable.from(purchases).scan(0, (total, amount) => total + amount)

balances
  .zip(purchases, (balance, purchase) => ({ purchase, balanceAfter: balance }))
  .toArray()
// => [
//      { purchase: 12, balanceAfter: 12 },
//      { purchase: 7, balanceAfter: 19 },
//      { purchase: 5, balanceAfter: 24 }
//    ]
```

### `select`
Project values. When working with objects you can reshape the data into entirely new structures.

```js
const invoices = [
  { id: 'INV-001', customer: 'Ada', lineItems: 3, total: 125.5 },
  { id: 'INV-002', customer: 'Grace', lineItems: 1, total: 42 },
]

Enumerable.from(invoices)
  .select((invoice, index) => ({
    display: `${index + 1}. ${invoice.customer} (${invoice.id})`,
    hasMultipleLines: invoice.lineItems > 1,
    totalWithTax: +(invoice.total * 1.07).toFixed(2),
  }))
  .toArray()
/* => [
  { display: '1. Ada (INV-001)', hasMultipleLines: true, totalWithTax: 134.29 },
  { display: '2. Grace (INV-002)', hasMultipleLines: false, totalWithTax: 44.94 }
] */
```

### `selectMany`
Flatten inner sequences. Works with arrays, iterables, and array-like objects.

```js
const letters = ['ab', 'cd']
Enumerable.from(letters).selectMany(word => word.split('')).toArray()
// => ['a', 'b', 'c', 'd']

Enumerable.from([{ id: 1, tags: ['x', 'y'] }])
  .selectMany(item => item.tags, (item, tag) => `${item.id}:${tag}`)
  .toArray()
// => ['1:x', '1:y']
```

### `where`
Filter elements.

```js
Enumerable.range(1, 6).where(x => x % 2 === 0).toArray()
// => [2, 4, 6]
```

### `choose`
Return only projected values that are neither `null` nor `undefined`.

```js
Enumerable.from([1, null, 3]).choose(x => (x ? x * 2 : null)).toArray()
// => [2, 6]
```

### `ofType`
Filter by runtime type.

```js
Enumerable.from([1, 'two', 3]).ofType(Number).toArray()
// => [1, 3]
```

## Combining Sequences

### `zip`
Combine values positionally. The resulting sequence is as long as the shortest input, making `zip` ideal for pairing related
streams that advance together: coordinates, ids with values, or even separate feeds that you want to inspect side-by-side.

```js
// Assemble addresses from separate sequences of house numbers and street names.
Enumerable.range(100, 3)
  .zip(['Pine Ave', 'Maple St', 'Elm Rd'], (house, street) => `${house} ${street}`)
  .toArray()
// => ['100 Pine Ave', '101 Maple St', '102 Elm Rd']
```

You can also use `zip` with `skip` and `scan` to build higher-level insights. The next example keeps a running total of
orders while exposing each individual order amount so the consumer can see both numbers at once:

```js
const orders = [29.99, 12.5, 48.0]
const runningTotal = Enumerable.from(orders).scan(0, (total, amount) => total + amount)

runningTotal
  .zip(orders, (total, order) => `Order: $${order.toFixed(2)}, cumulative: $${total.toFixed(2)}`)
  .toArray()
// => [
//      'Order: $29.99, cumulative: $29.99',
//      'Order: $12.50, cumulative: $42.49',
//      'Order: $48.00, cumulative: $90.49'
//    ]
```

### `merge`
Interleave multiple sequences.

```js
Enumerable.range(1, 3).merge(['a', 'b', 'c']).toArray()
// => [1, 'a', 2, 'b', 3, 'c']
```

### `join`
Inner join sequences.

```js
const people = [
  { id: 1, name: 'Ada', cityId: 1 },
  { id: 2, name: 'Alan', cityId: 2 },
]
const cities = [
  { id: 1, name: 'London' },
  { id: 2, name: 'Manchester' },
]

Enumerable.from(people)
  .join(
    cities,
    p => p.cityId,
    c => c.id,
    (p, c) => `${p.name} - ${c.name}`
  )
  .toArray()
// => ['Ada - London', 'Alan - Manchester']
```

### `leftJoin`
Include left-side rows even when there is no match.

```js
Enumerable.from(people)
  .leftJoin(
    [{ id: 1, name: 'London' }],
    p => p.cityId,
    c => c.id,
    (p, c) => `${p.name} - ${(c && c.name) || 'Unknown'}`
  )
  .toArray()
// => ['Ada - London', 'Alan - Unknown']
```

### `groupJoin`
Group inner matches per element.

```js
Enumerable.from(cities)
  .groupJoin(
    people,
    c => c.id,
    p => p.cityId,
    (c, residents) => ({ city: c.name, residents: residents.select(r => r.name).toArray() })
  )
  .toArray()
// => [{ city: 'London', residents: ['Ada'] }, { city: 'Manchester', residents: ['Alan'] }]
```

## Quantifiers and Set Logic

### `all`
Check whether every element matches a predicate.

```js
Enumerable.range(1, 5).all(x => x < 10)
// => true
```

### `any`
Detect if the sequence has any (or matching) items.

```js
Enumerable.empty().any()
// => false
Enumerable.range(1, 5).any(x => x > 3)
// => true
```

### `isEmpty`
Explicit emptiness test.

```js
Enumerable.range(1, 3).skip(3).isEmpty()
// => true
```

### `concat`
Append additional sequences.

```js
Enumerable.range(1, 2).concat([3, 4]).toArray()
// => [1, 2, 3, 4]
```

### `insert`
Insert another sequence at a specific index.

```js
Enumerable.range(1, 4).insert(2, Enumerable.range(100, 2)).toArray()
// => [1, 2, 100, 101, 3, 4]
```

### `alternate`
Alternate between elements or sequences.

```js
Enumerable.range(1, 3).alternate(0).toArray()
// => [1, 0, 2, 0, 3]

Enumerable.range(1, 3).alternate(['A', 'B']).toArray()
// => [1, 'A', 'B', 2, 'A', 'B', 3]
```

### `contains`
Test for existence optionally using a compare selector.

```js
Enumerable.from(['Ada', 'Alan']).contains('ada', name => name.toLowerCase())
// => true
```

### `defaultIfEmpty`
Provide a fallback when the sequence is empty.

```js
Enumerable.empty().defaultIfEmpty('none').toArray()
// => ['none']
```

### `distinct` and `distinctUntilChanged`
Remove duplicates globally or consecutive duplicates.

```js
Enumerable.from([1, 2, 2, 3]).distinct().toArray()
// => [1, 2, 3]

Enumerable.from([1, 1, 2, 1]).distinctUntilChanged().toArray()
// => [1, 2, 1]

Enumerable.from(['Ada', 'alan']).distinct(name => name.toLowerCase()).toArray()
// => ['Ada']
```

### `except`, `intersect`, `union`
Set-based operations with optional custom selectors.

```js
const primary = Enumerable.from([1, 2, 3, 4])
const secondary = [3, 4, 5]
primary.except(secondary).toArray() // => [1, 2]
primary.intersect(secondary).toArray() // => [3, 4]
primary.union(secondary).toArray() // => [1, 2, 3, 4, 5]

Enumerable.from(['Ada']).union(['alan'], name => name.toLowerCase()).toArray()
// => ['Ada']
```

### `sequenceEqual`
Check if two sequences have identical order and values.

```js
Enumerable.range(1, 3).sequenceEqual([1, 2, 3])
// => true

Enumerable.from(['Ada']).sequenceEqual(['ada'], name => name.toLowerCase())
// => true
```

## Ordering

### `orderBy`, `orderByDescending`
Sort sequences.

```js
const languages = [
  { name: 'Ada', year: 1980 },
  { name: 'C', year: 1972 },
  { name: 'Python', year: 1991 },
]

const ordered = Enumerable.from(languages).orderBy(lang => lang.year)
ordered.select(lang => lang.name).toArray()
// => ['C', 'Ada', 'Python']

Enumerable.from(languages)
  .orderByDescending(lang => lang.name)
  .select(lang => lang.name)
  .toArray()
// => ['Python', 'C', 'Ada']
```

### `IOrderedEnumerable.thenBy` / `thenByDescending`
Tie-break sorting.

```js
Enumerable.from([
  { name: 'Ada', group: 2 },
  { name: 'Alan', group: 1 },
  { name: 'Alonzo', group: 1 },
])
  .orderBy(person => person.group)
  .thenBy(person => person.name)
  .select(p => `${p.group}-${p.name}`)
  .toArray()
// => ['1-Alan', '1-Alonzo', '2-Ada']

Enumerable.from([
  { name: 'Grace', group: 1 },
  { name: 'Guido', group: 1 },
])
  .orderBy(person => person.group)
  .thenByDescending(person => person.name)
  .select(p => p.name)
  .toArray()
// => ['Guido', 'Grace']
```

### `IOrderedEnumerable.createOrderedEnumerable`
Build a custom ordering directly.

```js
const custom = Enumerable.from(['b', 'a', 'c'])
  .orderBy(x => x)
  .createOrderedEnumerable(x => x.charCodeAt(0), undefined, true)
custom.toArray()
// => ['c', 'b', 'a'] (descending by char code)
```

### `reverse`, `shuffle`, `weightedSample`

```js
Enumerable.range(1, 3).reverse().toArray() // => [3, 2, 1]
Enumerable.range(1, 5).shuffle().count() // => 5 (random order)
Enumerable.from([{ n: 'A', w: 1 }, { n: 'B', w: 5 }])
  .weightedSample(x => x.w)
  .take(3)
  .select(x => x.n)
  .toArray()
// => Biased toward 'B'
```

## Grouping

### `groupBy`

```js
Enumerable.from(['ant', 'bear', 'cat'])
  .groupBy(word => word.length, word => word.toUpperCase())
  .select(group => ({ length: group.key(), words: group.toArray() }))
  .toArray()
// => [{ length: 3, words: ['ANT', 'CAT'] }, { length: 4, words: ['BEAR'] }]

Enumerable.from(['aa', 'bb'])
  .groupBy(
    word => word,
    word => word,
    (key, group) => ({ key, group: group.toArray() }),
    key => key.length
  )
  .toArray()
// => groups deduplicated by key length
```

### `partitionBy`
Split a sequence into contiguous chunks keyed by a selector. Unlike `groupBy`, partition boundaries matter—use it when you
need to preserve run-length information such as streaks, log sessions, or state transitions.

```js
const sensorReadings = [
  { value: 23, status: 'ok' },
  { value: 24, status: 'ok' },
  { value: 28, status: 'alert' },
  { value: 26, status: 'alert' },
  { value: 25, status: 'ok' }
]

Enumerable.from(sensorReadings)
  .partitionBy(reading => reading.status)
  .select(group => ({ status: group.key(), count: group.count() }))
  .toArray()
// => [
//      { status: 'ok', count: 2 },
//      { status: 'alert', count: 2 },
//      { status: 'ok', count: 1 }
//    ]
```

You can also project items and aggregate within each partition while preserving their original order.

```js
Enumerable.from('AAABBBCCDA')
  .partitionBy(char => char)
  .select(group => `${group.key()}:${group.count()}`)
  .toJoinedString(',')
// => 'A:3,B:3,C:2,D:1,A:1'
```

### `buffer`
Collect items into fixed-size chunks.

```js
Enumerable.range(1, 7).buffer(3).toArray()
// => [[1, 2, 3], [4, 5, 6], [7]]
```

## Aggregation

### `aggregate`

Use a custom accumulator to reduce the sequence. You can omit the seed to start from the first element or supply one to build
up richer structures before optionally projecting the final result.

```js
// Reduction without an explicit seed: factorial of 5
Enumerable.range(1, 5).aggregate((product, value) => product * value)
// => 120

// Grow a richer object with a seed and project the final output
const orders = [
  { id: 1, region: 'NA', total: 25 },
  { id: 2, region: 'EU', total: 32 },
  { id: 3, region: 'NA', total: 18 },
]

Enumerable.from(orders)
  .aggregate(
    { count: 0, totalsByRegion: new Map() },
    (summary, order) => {
      summary.count++
      summary.totalsByRegion.set(
        order.region,
        (summary.totalsByRegion.get(order.region) ?? 0) + order.total,
      )
      return summary
    },
    summary => ({
      orderCount: summary.count,
      highestRegion: [...summary.totalsByRegion.entries()].sort((a, b) => b[1] - a[1])[0],
    }),
  )
// => { orderCount: 3, highestRegion: ['NA', 43] }
```

### `average`, `count`, `max`, `min`, `maxBy`, `minBy`, `sum`

```js
const values = Enumerable.from([1, 2, 3, 4])
values.average() // => 2.5
values.count() // => 4
values.max() // => 4
values.min() // => 1
values.sum() // => 10

const peopleAges = Enumerable.from([
  { name: 'Ada', age: 36 },
  { name: 'Grace', age: 40 },
])
peopleAges.maxBy(p => p.age) // => { name: 'Grace', age: 40 }
peopleAges.minBy(p => p.age) // => { name: 'Ada', age: 36 }
```

## Element Access

### `elementAt`, `elementAtOrDefault`

```js
const seq = Enumerable.range(1, 3)
seq.elementAt(1) // => 2
seq.elementAtOrDefault(10, 99) // => 99
```

### `first`, `firstOrDefault`

```js
Enumerable.range(1, 5).first(x => x > 3) // => 4
Enumerable.empty().firstOrDefault(0) // => 0
```

### `last`, `lastOrDefault`

```js
Enumerable.range(1, 5).last() // => 5
Enumerable.range(1, 5).lastOrDefault(x => x > 10, -1) // => -1
```

### `single`, `singleOrDefault`

```js
Enumerable.range(1, 5).where(x => x === 3).single() // => 3
Enumerable.empty().singleOrDefault('none') // => 'none'
```

## Partitioning

### `skip`, `skipWhile`

```js
Enumerable.range(1, 5).skip(2).toArray() // => [3, 4, 5]
Enumerable.range(1, 5).skipWhile(x => x < 3).toArray() // => [3, 4, 5]
```

### `take`, `takeWhile`

```js
Enumerable.range(1, 5).take(2).toArray() // => [1, 2]
Enumerable.range(1, 5).takeWhile(x => x < 4).toArray() // => [1, 2, 3]
```

### `takeExceptLast`, `takeFromLast`

```js
Enumerable.range(1, 5).takeExceptLast().toArray() // => [1, 2, 3, 4]
Enumerable.range(1, 5).takeExceptLast(2).toArray() // => [1, 2, 3]
Enumerable.range(1, 5).takeFromLast(2).toArray() // => [4, 5]
```

### `indexOf`, `lastIndexOf`

```js
Enumerable.from(['a', 'b', 'a']).indexOf('a') // => 0
Enumerable.from(['a', 'b', 'a']).lastIndexOf('a') // => 2
Enumerable.from([1, 2, 3]).indexOf(x => x === 2) // => 1
```

## Conversion

### `asEnumerable`, `cast`

```js
const array = [1, 2, 3]
const seqAgain = Enumerable.from(array).asEnumerable()
seqAgain.cast(Number).sum() // => 6
```

### `toArray`, `toLookup`, `toObject`

```js
Enumerable.range(1, 3).toArray() // => [1, 2, 3]

const lookup = Enumerable.from(['ant', 'ape']).toLookup(word => word[0])
lookup.get('a').toArray() // => ['ant', 'ape']

Enumerable.from(['Ada', 'Alan']).toLookup(
  person => person[0],
  person => person.toUpperCase(),
  key => key.toLowerCase()
)
// => lookup with uppercase values and case-insensitive keys

Enumerable.from(['Ada', 'Alan']).toObject(name => name[0], name => name)
// => { A: 'Alan' } (last wins)
```

### `toDictionary`

```js
const dict = Enumerable.from([
  { key: 'a', value: 1 },
  { key: 'b', value: 2 },
]).toDictionary(item => item.key, item => item.value)

dict.get('a') // => 1

const insensitive = Enumerable.from(['Ada', 'alan']).toDictionary(
  name => name,
  name => name.length,
  key => key.toLowerCase()
)
insensitive.get('ALAN') // => 4
```

### `toJSONString`

```js
Enumerable.range(1, 3).toJSONString()
// => '[1,2,3]'

Enumerable.range(1, 3).toJSONString(null, 2)
// => "[\n  1,\n  2,\n  3\n]"

Enumerable.from([{ n: 1 }, { n: 2 }]).toJSONString(['n'])
// => '[{"n":1},{"n":2}]'
```

### `toJoinedString`

```js
Enumerable.range(1, 3).toJoinedString(', ')
// => '1, 2, 3'
Enumerable.range(1, 3).toJoinedString(' - ', (value, index) => `${index}:${value}`)
// => '0:1 - 1:2 - 2:3'
```

### `doAction`, `forEach`, `force`

```js
let seen = []
const lazy = Enumerable.range(1, 3).doAction(x => seen.push(x))
lazy.first() // doAction runs lazily
seen // => [1]

Enumerable.range(1, 3).forEach((value, index) => {
  if (index === 1) return false // stop early
})

lazy.force() // iterate and cache
```

### `letBind`
Re-use the same evaluated sequence inside a function.

```js
Enumerable.range(1, 4)
  .letBind(seq => seq.select(x => x * 2))
  .toArray()
// => [2, 4, 6, 8]

Enumerable.range(1, 3)
  .letBind(seq => seq.toArray())
  .toArray()
// => [1, 2, 3] (array result treated as sequence)
```

### `share` and `memoize`
Share a single enumeration or cache results.

```js
const shared = Enumerable.generate(() => Math.random(), 3).share()
shared.toArray() // => e.g. [0.3, 0.7, 0.9]
shared.dispose() // free resources

const memoized = Enumerable.generate(() => Math.random(), 3).memoize()
memoized.toArray() === memoized.toArray() // => true (cached)
memoized.dispose()
```

### `catchError`, `finallyAction`

```js
let finallyCalled = false
Enumerable.from([1, 0, 2])
  .select(x => 10 / x)
  .catchError(err => console.log('Handled', err.message))
  .finallyAction(() => { finallyCalled = true })
  .toArray()
// => logs error, sets finallyCalled = true

Enumerable.from([1])
  .select(x => { throw new Error('boom') })
  .catchError('Error logged to console via string handler')
  .toArray()
```

### `log` and `trace`
Emit diagnostics while keeping the sequence lazy.

```js
Enumerable.range(1, 3).log().toArray()
// => logs each value
Enumerable.range(1, 3).log(x => x * 2).toArray()
// => logs doubled values
Enumerable.range(1, 3).trace('value', x => x * 10).toArray()
// => logs 'value: 10', 'value: 20', ...
```

## Specialized Interfaces

### `IDisposableEnumerable`
The result of `share()` or `memoize()` exposes `dispose()`.

```js
const resource = Enumerable.generate(() => Date.now(), 2).share()
resource.dispose()
```

### `IDictionary`

```js
const dictionary = Enumerable.empty().toDictionary()
dictionary.add('a', 1)
dictionary.set('b', 2)
dictionary.contains('a') // => true
dictionary.count() // => 2
dictionary.toEnumerable().select(kvp => `${kvp.key}:${kvp.value}`).toArray()
// => ['a:1', 'b:2']
dictionary.remove('a')
dictionary.clear()
```

### `ILookup` & `IGrouping`

```js
const lettersLookup = Enumerable.from(['apple', 'avocado', 'banana']).toLookup(word => word[0])
lettersLookup.count() // => 2
lettersLookup.contains('b') // => true
lettersLookup
  .toEnumerable()
  .select(group => ({ key: group.key(), source: group.getSource() }))
  .toArray()
// => [{ key: 'a', source: ['apple', 'avocado'] }, { key: 'b', source: ['banana'] }]
```

### `Utils`
The `Enumerable.Utils` helpers are available for advanced scenarios:

```js
const lambda = Enumerable.Utils.createLambda('x => x * 2')
lambda(10) // => 20
Enumerable.Utils.hasNativeIteratorSupport() // => true in modern environments

const customEnumerable = Enumerable.Utils.createEnumerable(() => {
  let current = 0
  return Enumerable.Utils.createEnumerator(
    () => { current = 0 },
    function() { return current < 3 ? this.yieldReturn(++current) : this.yieldBreak() },
    () => {}
  )
})
customEnumerable.toArray() // => [1, 2, 3]

Enumerable.Utils.extendTo(Array)
// ... arrays now have LINQ methods; undo with:
Enumerable.Utils.recallFrom(Array)
```

This guide now covers every operator provided by `linq.js`. Mix and match them to express complex data transformations succinctly.
