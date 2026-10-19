
the query planner for sparql has some standing rules for all modifications:

* It must be based on equivalence passes: a query plan  gets built from the
  sparql query and gets refined thru modular equivalence passes: a query
  plan gets transformed into a more efficient query plan that is still
  equivalent with the original one.

* The equivalence passes must carry a proof that the equivalence is
  guaranteed, or must at least be auditable

* No hacks to make specific queries work. Every improvement must be generic

* The query plan optimisation must be human understandable, when calling
  tostring on the raw and refined plan, it must be readable what happens.

* The query plan datastructure must remain nest friendly


