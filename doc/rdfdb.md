The central module of the RDF infrastructure is library(semweb/rdf_db).
It provides storage and indexed querying of RDF triples. RDF data is
stored as quintuples. The first three elements denote the RDF triple.
The extra _Graph_ and _Line_ elements provide information about the
origin of the triple.

The actual storage is provided by the _|foreign language (C)|_ module.
Using a dedicated C-based implementation we can reduce memory usage and
improve indexing capabilities, for example by providing a dedicated
index to support entailment over =|rdfs:subPropertyOf|=. Currently the
following indexes are provided (S=subject, P=predicate, O=object,
G=graph):

  * S, P, O, SP, PO, SPO, G, SG, PG

  * Predicates connected by *|rdfs:subPropertyOf|* are combined
    in a _|predicate cloud|_.  The system causes multiple
    predicates in the cloud to share the same hash.  The cloud
    maintains a 2-dimensional array that expresses the
    closure of all =|rdfs:subPropertyOf|= relations.  This
    index supports rdf_has/3 to query a property and all its
    children efficiently.

  * Additional indexes for predicates, resources and graphs allow
    enumerating these objects without duplicates.  For example,
    using rdf_resource/1 we enumerate all resources in the database
    only once, while enumeration using e.g., =|(rdf(R,_,_);rdf(_,_,R))|=
    normally produces many duplicate answers.

  * Literal _Objects_ are combined in a _|skip list|_ after case
    normalization. This provides for efficient case-insensitive search,
    prefix and range search. The plugin library library(semweb/litindex)
    provides indexed search on tokens inside literals.

## Query the RDF database {#semweb-query}

  * [[rdf/3]]
  * [[rdf/4]]
  * [[rdf_has/3]]
  * [[rdf_has/4]]
  * [[rdf_reachable/3]]
  * [[rdf_reachable/5]]

## Enumerating objects {#semweb-enumerate}

The predicates below enumerate the basic objects  of the RDF store. Most
of these predicates also enumerate objects   that  are not associated to
any currently visible triple. Objects are retained   as long as they are
visible in active queries or _snapshots_. After that, some are reclaimed
by the RDF garbage collector, while others are never reclaimed.

  * [[rdf_subject/1]]
  * [[rdf_resource/1]]
  * [[rdf_current_predicate/1]]
  * [[rdf_current_literal/1]]
  * [[rdf_graph/1]]
  * [[rdf_current_ns/2]]

## Modifying the RDF database {#semweb-modify}

The predicates below modify the RDF   store  directly. In addition, data
may be loaded using rdf_load/2  or   by  restoring a persistent database
using rdf_attach_db/2. Modifications follow the  Prolog _|logical update
view|_ semantics, which implies that   modifications remain invisible to
already  running  queries.  Further  isolation  can  be  achieved  using
rdf_transaction/3.

  * [[rdf_assert/3]]
  * [[rdf_assert/4]]
  * [[rdf_retractall/3]]
  * [[rdf_retractall/4]]
  * [[rdf_update/4]]

## Update view, transactions and snapshots {#semweb-update-view}

The update semantics of the RDF database follows the conventional Prolog
_|logical update view|_. In addition, the RDF database supports
_transactions_ and _snapshots_.

  * [[rdf_transaction/1]]
  * [[rdf_transaction/2]]
  * [[rdf_transaction/3]]
  * [[rdf_snapshot/1]]
  * [[rdf_delete_snapshot/1]]
  * [[rdf_active_transaction/1]]
  * [[rdf_current_snapshot/1]]

## Type checking predicates {#semweb-typecheck}

  * [[rdf_is_resource/1]]
  * [[rdf_is_bnode/1]]
  * [[rdf_is_literal/1]]

## Loading and saving to file {#semweb-load-save}

The RDF library can read and write triples in RDF/XML and a proprietary
binary format. There is a plugin interface defined to support additional
formats.  The library(semweb/turtle) uses this plugin API to support
loading Turtle files using rdf_load/2.

  * [[rdf_load/1]]
  * [[rdf_load/2]]
  * [[rdf_unload/1]]
  * [[rdf_save/1]]
  * [[rdf_save/2]]
  * [[rdf_make/0]]

### Partial save {#semweb-partial-save}

Sometimes it is necessary to make more arbitrary selections of material
to be saved or exchange RDF descriptions over an open network link. The
predicates in this section provide for this. Character encoding issues
are derived from the encoding of the _Stream_, providing support for
=utf8=, =iso_latin_1= and =ascii=.

  * [[rdf_save_header/2]]
  * [[rdf_save_footer/1]]
  * [[rdf_save_subject/3]]

### Fast loading and saving {#semweb-fast-load-save}

Loading and saving RDF format is relatively slow. For this reason we
designed a binary format that is more compact, avoids the complications
of the RDF parser and avoids repetitive lookup of (URL) identifiers.
Especially the speed improvement of about 25 times is worth-while when
loading large databases. These predicates are used for caching by
rdf_load/2 under certain conditions as well as for maintaining
persistent snapshots of the database using
library(semweb/rdf_persistency).

  * [[rdf_save_db/2]]
  * [[rdf_load_db/1]]


## Graph manipulation {#semweb-graphs}

Many RDF stores turned triples into quadruples. This store is no
exception, initially using the 4th argument to store the filename from
which the triple was loaded. Currently, the 4th argument is the RDF
_|named graph|_. A named graph maintains some properties, notably to
track origin, changes and modified state.

  * [[rdf_create_graph/1]]
  * [[rdf_unload_graph/1]]
  * [[rdf_graph_property/2]]
  * [[rdf_set_graph/2]]


## Literal matching and indexing {#semweb-literals}

Literal values are ordered and indexed using a _|skip list|_. The aim of
this index is threefold.

  * Unlike hash-tables, binary trees allow for efficient
    _prefix_ and _range_ matching. Prefix matching
    is useful in interactive applications to provide feedback
    while typing such as auto-completion.
  * Having a table of unique literals we generate creation and
    destruction events (see rdf_monitor/2).  These events can
    be used to maintain additional indexing on literals, such
    as `by word'.  See library(semweb/litindex).

As string literal matching is most frequently used for searching
purposes, the match is executed case-insensitive and after removal of
diacritics. Case matching and diacritics removal is based on Unicode
character properties and independent from the current locale. Case
conversion is based on the `simple uppercase mapping' defined by Unicode
and diacritic removal on the `decomposition type'. The approach is
lightweight, but somewhat simpleminded for some languages. The tables
are generated for Unicode characters upto 0x7fff. For more information,
please check the source-code of the mapping-table generator
=|unicode_map.pl|= available in the sources of this package.

Currently the total order of literals is first based on the type of
literal using the ordering _|numeric < string < term|_ Numeric values
(integer and float) are ordered by value, integers preceed floats if
they represent the same value. Strings are sorted alphabetically after
case-mapping and diacritic removal as described above. If they match
equal, uppercase preceeds lowercase and diacritics are ordered on their
unicode value. If they still compare equal literals without any
qualifier preceeds literals with a type qualifier which preceeds
literals with a language qualifier. Same qualifiers (both type or both
language) are sorted alphabetically.

The ordered tree is used for indexed execution of
literal(prefix(Prefix), Literal) as well as literal(like(Like), Literal)
if _Like_ does not start with a `*'. Note that results of queries that
use the tree index are returned in alphabetical order.

## Predicate properties {#semweb-predicates}

The predicates below form an experimental interface to provide more
reasoning inside the kernel of the rdb_db engine. Note that =symetric=,
=inverse_of= and =transitive= are not yet supported by the rest of the
engine. Also note that there is no relation to defined RDF properties.
Properties that have no triples are not reported by this predicate,
while predicates that are involved in triples do not need to be defined
as an instance of rdf:Property.

  * [[rdf_set_predicate/2]]
  * [[rdf_predicate_property/2]]

## Prefix Handling {#semweb-prefixes}

Prolog code often contains references to constant resources with a known
_prefix_ (also known as XML _namespaces_). For example,
=|http://www.w3.org/2000/01/rdf-schema#Class|= refers to the most
general notion of an RDFS class. Readability and maintability concerns
require for abstraction here. The RDF database maintains a table of
known _prefixes_. This table can be queried using rdf_current_ns/2 and
can be extended using rdf_register_ns/3. The prefix database is used to
expand =|prefix:local|= terms that appear as arguments to calls which
are known to accept a _resource_. This expansion is achieved by Prolog
preprocessor using expand_goal/2.

  * [[rdf_current_prefix/2]]
  * [[rdf_register_prefix/2]]

_Explicit_ expansion is achieved using the predicates below. The
predicate rdf_equal/2 performs this expansion at compile time, while the
other predicates do it at runtime.

  * [[rdf_equal/2]]
  * [[rdf_global_id/2]]
  * [[rdf_global_object/2]]
  * [[rdf_global_term/2]]

### Namespace handling for custom predicates {#semweb-meta}

If we implement a new predicate based on one of the predicates of the
semweb libraries that expands namespaces, namespace expansion is not
automatically available to it. Consider the following code computing the
number of distinct objects for a certain property on a certain object.

  ==
  cardinality(S, P, C) :-
        (   setof(O, rdf_has(S, P, O), Os)
        ->  length(Os, C)
        ;   C = 0
        ).
  ==

Now assume we want to write labels/2 that returns the number of distict
labels of a resource:

  ==
  labels(S, C) :-
        cardinality(S, rdfs:label, C).
  ==

This code will _not_ work because =|rdfs:label|= is not expanded at
compile time. To make this work, we need to add an rdf_meta/1
declaration.

  ==
  :- rdf_meta
        cardinality(r,r,-).
  ==

  * [[rdf_meta/1]]

The example below defines the rule concept/1.

  ==
  :- use_module(library(semweb/rdf_db)).  % for rdf_meta
  :- use_module(library(semweb/rdfs)).    % for rdfs_individual_of

  :- rdf_meta
	  concept(r).

  %%      concept(?C) is nondet.
  %
  %       True if C is a concept.

  concept(C) :-
	  rdfs_individual_of(C, skos:'Concept').
  ==

In addition to expanding _calls_, rdf_meta/1   also  causes expansion of
_|clause heads|_ for predicates  that  match   a  declaration.  This  is
typically used write Prolog statements   about  resources. The following
example produces three clauses with expanded (single-atom) arguments:

  ==
  :- use_module(library(semweb/rdf_db)).

  :- rdf_meta
	  label_predicate(r).

  label_predicate(rdfs:label).
  label_predicate(skos:prefLabel).
  label_predicate(skos:altLabel).
  ==

## Miscellaneous predicates {#semweb-misc}

This section describes the remaining predicates of the
library(semweb/rdf_db) module.

  * [[rdf_bnode/1]]
  * [[rdf_source_location/2]]
  * [[rdf_generation/1]]
  * [[rdf_estimate_complexity/4]]
  * [[rdf_statistics/1]]
  * [[rdf_match_label/3]]
  * [[lang_matches/2]]
  * [[lang_equal/2]]
  * [[rdf_reset_db/0]]
  * [[rdf_version/1]]


## Memory management considerations {#semweb-memory-management}

Storing RDF triples in main memory provides much better performance than
using external databases. Unfortunately, although memory is fairly cheap
these days, main memory is severely limited when compared to disks.
Memory usage breaks down to the following categories.  Rough estimates
of the memory usage is given *|for 64-bit systems|*.  32-bit system use
slightly more than half these amounts.

  - Actually storing the triples.  A triple is stored in a C struct
    of 144 bytes. This struct both holds the quintuple,
    some bookkeeping information and the 10 next-pointers for the (max)
    to hash tables.

  - The bucket array for the hashes. Each bucket maintains a
    _head_, and _tail_ pointer, as well as a count for the number of
    entries. The bucket array is allocated if a particular index is
    created, which implies the first query that requires the index.
    Each bucket requires 24 bytes.

    Bucket arrays are resized if necessary.  Old triples remain at
    their original location.  This implies that a query may need to
    scan multiple buckets.  The garbage collector may relocate old
    indexed triples.  It does so by copying the old triple.  The
    old triple is later reclaimed by GC.  Reindexed triples will
    be reused, but many reindexed triples may result in a significant
    memory fragmentation.

  - Resources are maintained in a separate table to support
    rdf_resource/1.  A resources requires approximately 32 bytes.

  - Identical literals are shared (see rdf_current_literal/1) and
    stored in a _|skip list|_.  A literal requires approximately
    40 bytes, excluding the atom used for the lexical representation.

  - Resources are stored in the Prolog atom-table.  Atoms with the
    average length of a resource require approximately 88 bytes.

The hash parameters can be controlled with rdf_set/1. Applications that
are tight on memory and for which the query characteristics are more or
less known can optimize performance and memory by fixing the
hash-tables. By fixing the hash-tables we can tailor them to the
frequent query patterns, we avoid the need for to check multiple hash
buckets (see above) and we avoid memory fragmentation due to optimizing
triples for resized hashes.

  ==
  set_hash_parameters :-
	rdf_set(hash(s,   size, 1048576)),
	rdf_set(hash(p,   size, 1024)),
	rdf_set(hash(sp,  size, 2097152)),
	rdf_set(hash(o,   size, 1048576)),
	rdf_set(hash(po,  size, 2097152)),
	rdf_set(hash(spo, size, 2097152)),
	rdf_set(hash(g,   size, 1024)),
	rdf_set(hash(sg,  size, 1048576)),
	rdf_set(hash(pg,  size, 2048)).
  ==

  * [[rdf_set/1]]

### The garbage collector {#semweb-gc}

The RDF store has a garbage collector that runs in a separate thread
named =__rdf_GC=.  The garbage collector removes the following objects:

  - Triples that have died before the the generation of last
    still active query.
  - Entailment matrices for =|rdfs:subPropertyOf|= relations that are
    related to old queries.

In addition, the garbage collector reindexes triples associated to the
hash-tables before the table was resized. The most recent resize
operation leads to the largest number of triples that require
reindexing, while the oldest resize operation causes the largest
slowdown. The parameter =optimize_threshold= controlled by rdf_set/1 can
be used to determine the number of most recent resize operations for
which triples will not be reindexed.  The default is 2.

Normally, the garbage collector does it job in the background at a low
priority.  The predicate rdf_gc/0 can be used to reclaim all garbage and
optimize all indexes.

### Warming up the database {#semweb-warming-up}

The RDF store performs many operations lazily or in background threads.
For maximum performance, perform the following steps:

  - Load all the data without doing queries or retracting data in
    between.  This avoids creating the indexes and therefore the need
    to resize them.

  - Perform each of the indexed queries.  The following call performs
    this.  Note that it is irrelevant whether or not the query succeeds.

    ==
    warm_indexes :-
	ignore(rdf(s, _, _)),
	ignore(rdf(_, p, _)),
	ignore(rdf(_, _, o)),
	ignore(rdf(s, p, _)),
	ignore(rdf(_, p, o)),
	ignore(rdf(s, p, o)),
	ignore(rdf(_, _, _, g)),
	ignore(rdf(s, _, _, g)),
	ignore(rdf(_, p, _, g)).
    ==

  - Duplicate adminstration is initialized in the background after the
    first call that returns a significant amount of duplicates. Creating
    the adminstration can be forced by calling rdf_update_duplicates/0.

Predicates:

  * [[rdf_gc/0]]
  * [[rdf_update_duplicates/0]]
