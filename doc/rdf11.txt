The library(semweb/rdf11) provides a new interface to the SWI-Prolog RDF
database based on the RDF 1.1 specification.

## Query the RDF database {#rdf11-query}

  * [[rdf/3]]
  * [[rdf_has/3]]
  * [[rdf_reachable/3]]

### Constraints on literal values

  * [[rdf_where/1]]

## Enumerating and testing objects {#rdf11-enum}

### Enumerating objects by role {#rdf11-enum-role}

  * [[rdf_subject/1]]
  * [[rdf_predicate/1]]
  * [[rdf_object/1]]
  * [[rdf_node/1]]
  * [[rdf_graph/1]]

### Enumerating objects by type {#rdf11-enum-type}

  * [[rdf_literal/1]]
  * [[rdf_bnode/1]]
  * [[rdf_iri/1]]
  * [[rdf_name/1]]
  * [[rdf_term/1]]

### Testing objects types {#rdf11-type-test}

  * [[rdf_is_iri/1]]
  * [[rdf_is_bnode/1]]
  * [[rdf_is_literal/1]]
  * [[rdf_is_name/1]]
  * [[rdf_is_object/1]]
  * [[rdf_is_predicate/1]]
  * [[rdf_is_subject/1]]
  * [[rdf_is_term/1]]

## RDF literals {#rdf11-literal}

  * [[rdf_canonical_literal/2]]
  * [[rdf_lexical_form/2]]
  * [[rdf_compare/3]]

## Accessing RDF graphs {#rdf11-graph}

  * [[rdf_default_graph/1]]

## Modifying the RDF store {#rdf11-modify}

  * [[rdf_assert/3]]
  * [[rdf_retractall/3]]
  * [[rdf_create_bnode/1]]

## Accessing RDF collections {#rdf11-collections}

The following predicates are utilities to access RDF 1.1 _collections_.
A collection is a linked list created from `rdf:first` and `rdf:next`
triples, ending in `rdf:nil`.

  * [[rdf_last/2]]
  * [[rdf_list/1]]
  * [[rdf_list/2]]
  * [[rdf_length/2]]
  * [[rdf_member/2]]
  * [[rdf_nth0/3]]
  * [[rdf_assert_list/2]]
  * [[rdf_retract_list/1]]
