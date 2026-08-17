# Element Identity for Inlined Multivalued Slots — Design

**Status:** Draft, awaiting user review before plan-writing.
**Date:** 2026-08-17
**Branch:** `feat/inlined-multivalued-element-identity`
**Supersedes:** the abandoned shapes work on branch `feat/container-shapes-and-verify-old` (see "Recyclable material" at the end).

## Goal

Answer the question, for each inlined multivalued shape: **where does element identity come from?**

The offered options are:

- a **key** (or identifier) — a slot on the element class,
- a **composed key** (content) — declared with the existing LinkML meta `unique_keys`,
- **opaque** — nowhere; the value is replaced as a whole.

"Where does identity for an element come from" should be easy to answer for a data model author.

## Non-goal

The inferred semantics do **not** change for downstream projects not opting in to the linter. The positional index path deltas produced today are still valuable for projects not dealing with multiple sources producing deltas for the same object at the same time.

## Proposed solution

Inlined multivalued slots cause ambiguous deltas downstream.

1. We add a **`diff.linkml.io/opaque` annotation**, with the meaning: **stop all recursion, replace the whole value.**
2. Together with **new support for the existing LinkML meta `unique_keys`**, allowing to declare composed keys.
3. An **opt-in linter** warns when your data model still allows for ambiguous deltas in multi-sourced operation.
4. We provide **examples explaining the nuanced options** a data model author has when the linter flags ambiguity (worked out below on the constituting examples).

Consequence of this strict mode is that some LinkML schemas need to be reworked to be compliant, and more cases of patches (produced outside this diff lib) cannot be applied fully.

## Problem statement

When the element class of an inlined multivalued slot carries no key or identifier, the diff falls back to positional index paths (`src/runtime/src/diff.rs:267-296` — `for i in 0..max_len`, numeric path segments). A positional path is only meaningful against the exact list the producer saw. In multi-sourced operation the golden record has drifted by the time a patch arrives, and the index silently selects a different element — no error, wrong data.

Both constituting examples are real, from the asset360 model at
`consolidator-server/components/py/asset360-model/asset360_model/schemas/asset360/repository/v1.0.0/`.

### Example 1 — phone numbers: data loss and duplication under current inferred semantics

`Service.hasPhoneNumber` is an inlined list of anonymous value objects (`tunnels.yaml:1954-1999`, locale annotations elided):

```yaml
  Service:
    attributes:
      hasPhoneNumber:
        description: Phone number of the resource.
        range: ServicePhoneNumber
        multivalued: true
        inlined_as_list: true

  ServicePhoneNumber:
    description: >-
      A phone number associated with a Service, classified by its function
      (emergency, non-urgent, operator, etc.). Inlined into Service.
    attributes:
      phoneNumber:
        range: string
      hasNumberFunction:
        range: NumberFunction
        required: true
```

`ServicePhoneNumber` has no `identifier`, no `key`, and the schema declares no `unique_keys` anywhere. Elements have no stable identity at all — yet the model *does* have an identity rule in mind. It lives in hand-written SHACL (`constraints.shacl.ttl:546-612`), invisible to LinkML:

```turtle
asset360:Service_OnePhoneNumberPerFunctionShape
  a sh:NodeShape ;
  sh:targetClass asset360:Service ;
  sh:message "A service cannot have two phone numbers with the same function."@en ;
  sh:property [
    sh:path asset360:hasPhoneNumber ;
    sh:qualifiedValueShape [ sh:path asset360:hasNumberFunction ; sh:hasValue "Emergency_Number" ] ;
    sh:qualifiedMaxCount 1
  ] ;
  # ... one qualifiedMaxCount block per NumberFunction value ...
```

So `hasNumberFunction` *behaves* as a key — but LinkML sees an unkeyed list, and the diff sees positions. (The ingestion layer even carries an external identity per phone number — `source_id_key: new_uri` in `changeset-generator/.../ce_kwoa_a1552/service_phone_number.yaml` — which is dropped at the schema boundary.)

**Data loss.** Two sources both derived their deltas from the same snapshot:

```json
"hasPhoneNumber": [
  {"phoneNumber": "09/241.25.00", "hasNumberFunction": "Emergency_Number"},
  {"phoneNumber": "09/241.25.03", "hasNumberFunction": "Non_Urgent_Communication"}
]
```

- Source A corrects the non-urgent number: `Update hasPhoneNumber/1/phoneNumber` → `"09/241.25.99"`.
- Source B removes the emergency number. Positionally that is a cascade: `Update hasPhoneNumber/0/*` (the non-urgent content shifts into index 0, with the *stale* phone number `09/241.25.03`) plus `Remove hasPhoneNumber/1`.

Apply A then B: B's cascade overwrites index 0 with the stale snapshot value — A's correction is silently reverted. Apply B then A: A's path `hasPhoneNumber/1` now points at nothing (or at whatever drifted in) — the edit is lost or lands on the wrong element. Either order corrupts; neither reports an error.

**Duplication.** Two ingest sources independently discover the same new operator number and both emit `Add hasPhoneNumber/2 = {"phoneNumber": "09/241.25.10", "hasNumberFunction": "Operator"}`. Both apply; the golden record now holds the same phone number twice. Under key-based identity the second add would have been recognised as the same element.

### Example 2 — `PositioningSystemCoordinate`: one class, two identity shapes

`PositioningSystemCoordinate` declares a key — `typeURI`, the type designator, which plays the role of the coordinate-system discriminator (`rsm.yaml:610-625`, `asset360.yaml:541-550`, elided):

```yaml
  PositioningSystemCoordinate:
    is_a: ObservableProperty
    description: A tuple of coordinates in a given positioning system.
    slots:
      - typeURI            # range: uri, designates_type: true
    attributes:
      PositioningSystemCoordinate_positioningSystem:
        range: PositioningSystem
    slot_usage:
      typeURI:
        key: true
```

**Shape B — keyed dict (the key is real).** `SpotLocation` holds at most one coordinate per coordinate-system type (`rsm.yaml:678-696`):

```yaml
  SpotLocation:
    is_a: BaseLocation
    attributes:
      SpotLocation_coordinates:
        multivalued: true
        inlined: true
        inlined_as_list: false     # JSON object keyed by typeURI
        range: PositioningSystemCoordinate
```

Real committed data (`asset360-model/tests/data/signal-obj-with-track.json:38-96`, trimmed) — one `LinearCoordinate`, one `GeographicCoordinate`, keyed by class URI:

```json
"SpotLocation_coordinates": {
  "https://data.infrabel.be/asset360-rsm-subset/LinearCoordinate": {
    "measure": {"Quantity_unit": ".../unit/Kilometer", "NumericQuantity_value": 526.0},
    "typeURI": "http://rsm.uic.org/RSM12#EAID_CB107995_3610_4622_824B_708281B24CEA"
  },
  "https://data.infrabel.be/asset360-rsm-subset/GeographicCoordinate": {
    "typeURI": "https://data.infrabel.be/asset360-rsm-subset/GeographicCoordinate",
    "latitude": 50.820240734349845,
    "longitude": 4.316083008990688
  }
}
```

~25k records rely on this keyed behaviour. (The same pattern recurs one level up: `locations` is a dict of `BaseLocation` keyed by the `locationrole` enum slot, `rsm.yaml:734-742`.)

**Shape A — vertex ring (the key is constant).** `Polyline` and `Polygon` hold the *same* class as an ordered ring (`rsm.yaml:567-608`):

```yaml
  Polyline:
    is_a: NamedResource
    attributes:
      PolyLine_coordinates:
        range: PositioningSystemCoordinate
        multivalued: true
        inlined_as_list: true

  Polygon:
    is_a: NamedResource
    attributes:
      Polygon_coordinates:
        range: PositioningSystemCoordinate
        multivalued: true
        inlined_as_list: true
```

Every vertex of a ring is the same coordinate subclass, so the declared key (`typeURI`) is **constant across elements**. Position is the only identity, ring data legitimately repeats the key value (a closed ring may even repeat a whole vertex), and vertex order is meaningful.

One class, two identity shapes. That ring data violates any `unique_keys` the class would declare — so the class can't declare one. **Remodelling into different classes is the only solution**: the keyed lookup usage keeps a class whose key/`unique_keys` declaration is truthful for all of its data, and the ring usage gets its own class (an un-keyed vertex) whose containers are declared opaque. We do not compromise on the goal by letting one class carry contradictory identity declarations per slot.

## The options when the linter flags a slot

The linter's question is always the same — *where does element identity come from?* — and the author has exactly three answers, plus a rework escape hatch. Worked out on the constituting examples:

### Option 1 — a key (or identifier): give elements the identity they already have

`hasNumberFunction` is already the de-facto key (the SHACL shape says so). Declare it:

```yaml
  ServicePhoneNumber:
    attributes:
      phoneNumber:
        range: string
      hasNumberFunction:
        range: NumberFunction
        key: true
        required: true
```

Deltas become key-addressed (`hasPhoneNumber/Emergency_Number/phoneNumber`), immune to drift and reorder; the duplicate-add collapses into an update of the same element. The SHACL uniqueness rule is now also expressed in the schema itself.

### Option 2 — a composed key (content): declare `unique_keys`

When no single slot identifies an element but a combination does, declare the existing LinkML meta `unique_keys` on the element class:

```yaml
  ServicePhoneNumber:
    unique_keys:
      phone_identity:
        unique_key_slots:
          - hasNumberFunction
          - phoneNumber
    attributes:
      ...
```

Element identity is the composed key — here effectively the element's content. Elements are matched across versions by that identity; an edit of a key-constituent is a remove-plus-add of the whole element, an edit of a non-key slot is addressed at the element. Reorder is not a change.

### Option 3 — opaque: identity comes from nowhere, say so

For the vertex ring, elements genuinely have no identity — a moved vertex, an inserted vertex, a reversed ring are all edits *of the geometry*, not of a vertex:

```yaml
      RingVertex_coordinates:
        range: RingVertex
        multivalued: true
        inlined_as_list: true
        annotations:
          diff.linkml.io/opaque: true
```

All recursion stops at the slot: any change below it is exactly one `Update` at the slot path carrying the whole old and new value. Two sources editing the same ring conflict visibly at the ring level — whole value against whole value — instead of interleaving vertex indices into a corrupt geometry.

### Option 4 — remodel: when one class needs two answers

`PositioningSystemCoordinate` needs option "key" in `SpotLocation` and option "opaque" in `Polyline`/`Polygon`, and its ring data violates the very declaration the keyed usage needs. Split the class: the keyed coordinate lookup keeps `PositioningSystemCoordinate` (with its `typeURI` key), the rings move to a dedicated vertex class with no key, held by opaque slots. Identity declarations stay class-level truths; slots choose only *whether* to recurse, never *what identity means*.

## Consequence of strict mode

- Some LinkML schemas need to be reworked to be compliant — in asset360 concretely: `ServicePhoneNumber` gains a key or `unique_keys`, and the `PositioningSystemCoordinate` ring/lookup dual use is split into two classes.
- More cases of patches produced outside this diff lib cannot be applied fully: a patch that addresses elements positionally has no meaning against an opaque slot (only whole-value updates apply) and no reliable meaning against a keyed/composed-key container. Such deltas are reported as failed rather than guessed at.

## Recyclable material

The abandoned branch `feat/container-shapes-and-verify-old` (local, single commit `312e6d8`) and the follow-up spike `spike/unique-keys-vs-opaque` contain fixtures and tests that carry over; the `DiffShape::Set`/`ShapeConfig` machinery itself is superseded by this design.

Recycled (near-verbatim, renamed to `opaque` where the branch says `array`):

- `diff_shapes.rs:158-219` — `array_slot_emits_one_whole_slot_update` (move / insert / drop / reverse a vertex ring, each exactly one whole-slot `Update`), the scalar-list variant, and `array_slot_unchanged_emits_nothing`.
- `diff_shapes.rs:419-463` — `keyed_slot_keeps_minimal_field_level_deltas`, as the regression guard that keyed containers keep minimal field-level deltas.
- `diff_shapes.rs:467-492` — `undeclared_slots_keep_positional_behaviour`, the non-goal's compatibility guard.
- `load_duplicate_keys.rs:113-181` — missing `key` is an error / missing `identifier` warns / type-designator carve-out, as linter severity fixtures (the designator carve-out exists precisely because of coordinate classes like Example 2).
- Multiplicity guards (`set_diff_respects_multiplicity`, the `["a","a"]` load assertion): a repeated element is data, never deduped.
- From the spike: `opaque_*` tests, `coordinates_match_by_unique_keys_derived_key`, and the `unique_keys` ambiguity-warning pair.

Not recycled: the `Set` content-matching diff/patch (`diff_set`, `apply_set_leaf_delta`, the drift-location trio) and the single-slot `shape_key` override — both replaced by `unique_keys`-declared identity.
