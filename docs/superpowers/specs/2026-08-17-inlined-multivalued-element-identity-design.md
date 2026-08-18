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

The correct resolution for this slot is Option 2 below: declare the SHACL rule as `unique_keys`.

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

Every vertex of a ring is the same coordinate subclass, so the declared key (`typeURI`) is **constant across elements**. Real committed data shows it plainly (`consolidator_api/goldenrecords/tests/data/a_trail.json`, trimmed to two of four vertices — all four repeat the identical `typeURI` and the identical positioning system):

```json
"polylines": [{
  "PolyLine_coordinates": [
    {"x": 214888.6, "y": 97029.71,
     "typeURI": "https://data.infrabel.be/asset360/MicroCoordinate",
     "PositioningSystemCoordinate_positioningSystem": {"typeURI": "http://rsm.uic.org/RSM12#EAID_4160AA98_..."}},
    {"x": 214819.42, "y": 97029.71,
     "typeURI": "https://data.infrabel.be/asset360/MicroCoordinate",
     "PositioningSystemCoordinate_positioningSystem": {"typeURI": "http://rsm.uic.org/RSM12#EAID_4160AA98_..."}}
  ]
}]
```

Position is the only identity, ring data legitimately repeats the key value (a closed ring may even repeat a whole vertex), and vertex order is meaningful. The per-vertex `typeURI` and positioning system are pure redundancy: the system is a property of the ring, not of the vertex.

One class, two identity shapes. That ring data violates any `unique_keys` the class would declare — so the class can't declare one, and letting the opaque annotation suppress a schema constraint would be a layering inversion we do not accept. The resolution is to rework the model: move the positioning-system identity up a layer and give rings a bare vertex class that never inherits the key. One valid remodeling is worked out in Option 4 below; `SpotLocation_coordinates` and the coordinate hierarchy stay untouched.

## The options when the linter flags a slot

The linter's question is always the same — *where does element identity come from?* — and the author has exactly three answers, plus a rework escape hatch. Each constituting example gets its correct resolution below.

### Option 1 — a key (or identifier): identity a single slot already provides

When the element class declares a `key` or `identifier` slot that is truthful for all of its data, nothing needs to change — the diff already matches elements by it. `SpotLocation_coordinates` is the example: a dict keyed by `typeURI`, at most one coordinate per positioning system, exactly what the data means.

### Option 2 — a composed key (content): declare `unique_keys` — the phone number solution

`hasNumberFunction` is already the de-facto identity — the SHACL shape says so. Declare exactly that rule with the existing LinkML meta `unique_keys`:

```yaml
  ServicePhoneNumber:
    unique_keys:
      one_number_per_function:
        unique_key_slots:
          - hasNumberFunction
    attributes:
      phoneNumber:
        range: string
      hasNumberFunction:
        range: NumberFunction
        required: true
```

This is the SHACL constraint expressed verbatim in the schema, and it is purely additive: `unique_keys` changes neither serialization nor loading of the deployed list data (unlike promoting the slot to `key: true`, which changes the container's serialization contract). Element identity is the function: deltas are addressed by it (`hasPhoneNumber/Emergency_Number/phoneNumber`), immune to drift and reorder, and the duplicate-add from Example 1 collides into the same element instead of duplicating it.

`unique_key_slots` composes: had the model allowed several numbers per function, `[hasNumberFunction, phoneNumber]` would make the full content the identity — at the price that every correction becomes a remove-plus-add of the whole element. Here that composition would be wrong: it would permit two numbers for the same function, contradicting the SHACL rule.

### Option 3 — opaque: identity comes from nowhere, say so

For the vertex ring, elements genuinely have no identity — a moved vertex, an inserted vertex, a reversed ring are all edits *of the geometry*, not of a vertex:

```yaml
      Polygon_coordinates:
        range: Vertex
        multivalued: true
        inlined_as_list: true
        annotations:
          diff.linkml.io/opaque: true
```

All recursion stops at the slot: any change below it is exactly one `Update` at the slot path carrying the whole old and new value. Two sources editing the same ring conflict visibly at the ring level — whole value against whole value — instead of interleaving vertex indices into a corrupt geometry.

### Option 4 — remodel: when one class needs two answers — the coordinate solution

`PositioningSystemCoordinate` needs Option 1 in `SpotLocation` and Option 3 in `Polyline`/`Polygon`, and its ring data violates the very declaration the keyed usage needs. No slot-level override can fix that — identity declarations stay class-level truths; slots choose only *whether* to recurse, never *what identity means*. Rework the model instead. One valid remodeling (shown for `Polygon`; `Polyline` is symmetric):

```yaml
  AreaLocation:
    attributes:
      polygons:
        range: Polygon
        multivalued: true
        inlined_as_list: true

  Polygon:
    is_a: NamedResource
    unique_keys:
      one_polygon_per_positioning_system:
        unique_key_slots:
          - positioningSystemType
    attributes:
      positioningSystemType:        # the identity the vertices used to repeat,
        range: uri                  # lifted up to the ring layer
        required: true
      Polygon_positioningSystem:
        range: PositioningSystem
        inlined: true
      Polygon_coordinates:
        range: Vertex
        multivalued: true
        inlined_as_list: true
        annotations:
          diff.linkml.io/opaque: true

  Vertex:
    attributes:
      x: {range: float}
      y: {range: float}
      z: {range: float}
```

The move is to introduce a layer so the key slot no longer sits on the elements holding the geometry data:

- `locations` is already keyed by `locationrole` — that layer exists.
- A polygon's identity within `polygons` is its positioning system, declared as a `unique_keys` composed key on `Polygon`. The schema's own description already says the list holds the same shape once per positioning system; the redundant per-vertex `typeURI` / positioning system move up to the ring layer, which is what they always described.
- The ring becomes an opaque list of a bare `Vertex` class that never inherits `key: typeURI`, so no declaration is violated by repeated vertices.
- `SpotLocation_coordinates` and the `PositioningSystemCoordinate` hierarchy are untouched; the ~25k keyed lookup records keep their behaviour.

## Consequence of strict mode

- Some LinkML schemas need to be reworked to be compliant — in asset360 concretely: `ServicePhoneNumber` gains a `unique_keys` declaration, and the polygon/polyline rings move their positioning-system identity up a layer and become opaque lists of a bare `Vertex` class.
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
