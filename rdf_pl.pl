:- module(rdf_pl,
          [ rdf/3,                              % ?S,?P,?O
            rdf/4,                              % ?S,?P,?O,?G
            rdf/5,                              % ?S,?P,?O,?G,?Gen
            rdf_subject/1,                      % ?S
            rdf_current_predicate/1,            % ?P
            rdf_assert/4,                       % +S,+P,+O,+G
            rdf_assert/3,                       % +S,+P,+O
            rdf_retractall/3,                   % ?S,?P,?O
            rdf_retractall/4,                   % ?S,?P,?O,?G
            rdf_reset_db/0,
            rdf_statistics/1
          ]).

/** <module> Create random graphs

Operations:
        - Create a property
        - Add/Del triples for a property
        - Add/Del a subPropertyOf relation
          - Extend a tree
          - Make multiple inheritance in a tree
          - Create cycle in a tree

Shadow DB is a graph. We do not need literals. Triples are represented
as

        triple(S,P,O,G,Born,Id)
        died(Id, Died)
*/

:- dynamic
    subject/1,
    predicate/1,
    triple/6,
    died/2,
    triple_id/1,
    generation/1.

rdf_reset_db :-
    retractall(subject(_)),
    retractall(predicate(_)),
    retractall(triple(_,_,_,_,_,_)),
    retractall(died(_,_)),
    retractall(triple_id(_)),
    retractall(generation(_)).

rdf_statistics(triples(Count)) :-
    predicate_property(triple(_,_,_,_,_,_), number_of_clauses(Count)).

rdf_assert(S,P,O) :-
    rdf_assert(S,P,O,user).

rdf_assert(S,P,O,G) :-
    with_mutex(triple_db, t_assert_(S,P,O,G)).


rdf(S,P,O) :-
    rdf(S,P,O,_).

rdf(S,P,O,G) :-
    generation(Gen),
    rdf(S,P,O,G,Gen).

rdf(S,P,O,G,Gen) :-
    rdf(S,P,O,G,Gen,_).

rdf(S,P,O,G,Gen,Id) :-
    triple(S,P,O,G,Born,Id),
    Gen >= Born,
    \+ (  died(Id, Died),
          Gen >= Died
       ).

rdf_subject(S) :-
    subject(S).

rdf_current_predicate(S) :-
    predicate(S).

rdf_retractall(S,P,O) :-
    rdf_retractall(S,P,O,_).

rdf_retractall(S,P,O,G) :-
    generation(Gen),
    with_mutex(triples,
               (   rdf(S,P,O,G,Gen,Id),
                   next_generation(Died),
                   assertz(died(Id, Died)),
                   fail
               ;   true
               )).

t_assert_(S,P,O,G) :-
    next_triple_id(Id),
    next_generation(Gen),
    assertz(triple(S,P,O,G,Gen,Id)),
    assert_subject(S),
    assert_predicate(P).

next_triple_id(Id) :-
    retract(triple_id(Id0)),
    !,
    Id is Id0+1,
    assertz(triple_id(Id)).
next_triple_id(1) :-
    assertz(triple_id(1)).

next_generation(Gen) :-
    retract(generation(Gen0)),
    !,
    Gen is Gen0+1,
    assertz(generation(Gen)).
next_generation(1) :-
    assertz(generation(1)).

assert_subject(S) :-
    subject(S),
    !.
assert_subject(S) :-
    assertz(subject(S)).

assert_predicate(P) :-
    predicate(P),
    !.
assert_predicate(P) :-
    assertz(predicate(P)).

