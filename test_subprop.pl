:- module(test_subprop,
	  [ test/0
	  ]).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(record)).
:- use_module(library(settings)).
:- use_module(library(debug)).
:- use_module(library(broadcast)).
:- use_module(random_graph).
:- use_module(search).

/** <module> Test RDF subproperty handling

This design assumes N properties with a   randomly changing set of edges
between them. Edges are  created  at  a   generation  and  killed  at  a
generation.  We maintain a shadow Prolog DB to verify the correctness of
the inferences made by the RDF DB.

The idea is to have a set   of properties and pseudo-randomly create and
remove subPropertyOf relations. For each  property,   we  have  a single
triple that has the same URI as the property: rdf(P,P,P). The properties
are named p1, p2, ... pN.
*/

:- debug(subprop).

:- dynamic
	predicate/2,			% Predicate, Gen
	sub_of/3,			% P1, P2, Born
	died/2.				% Born, Died

cleanup :-
	retractall(predicate(_,_)),
	retractall(sub_of(_,_,_)),
	retractall(died(_,_)),
	rdf_reset_db.

test :-
	show,
	reset,
	loop(1).

loop(I) :-
	step,
	check_all,
	succ(I, I2),
	format(user_error, '\r~t~D~6|', [I]),
	loop(I2).


:- listen(graph(Action), update_graph(Action)).

update_graph(Action) :-
	debug(subprop, '~p', [Action]).
update_graph(reset) :-
	cleanup.
update_graph(add_node(I)) :-
	atom_concat(p, I, P),
	rdf_assert(P,P,P),
	rdf_generation(Gen),
	assertz(predicate(P, Gen)).
update_graph(add_edge(SubI,SuperI)) :-
	atom_concat(p, SubI, Sub),
	atom_concat(p, SuperI, Super),
	rdf_assert(Sub, rdfs:subPropertyOf, Super),
	rdf_generation(Gen),
	assertz(sub_of(Sub,Super,Gen)).
update_graph(del_edge(SubI,SuperI)) :-
	atom_concat(p, SubI, Sub),
	atom_concat(p, SuperI, Super),
	rdf_retractall(Sub, rdfs:subPropertyOf, Super),
	rdf_generation(Gen),
	forall((sub_of(Sub,Super,Born),
		\+ died(Born, _)),
	       assertz(died(Born, Gen))).

check_all :-
	rdf_generation(Gen),
	check_all(Gen).

check_all(Gen) :-
	forall(predicate(P1,_),
	       forall(predicate(P2,_),
		      check_all(Gen, P1, P2))).

check_all(Gen, Sub, Super) :-
	(   subPropertyOf(Gen, Sub, Super)
	->  assertion(rdf_has(Sub, Super, Sub))
	;   assertion(\+ rdf_has(Sub, Super, Sub))
	).



subPropertyOf_1(Gen, Sub, Super) :-
	sub_of(Sub, Super, Born),
	Gen >= Born,
	\+ (  died(Born, Died),
	      Gen >= Died
	   ).

superPropertyOf_1(Gen, Super, Sub) :-
	subPropertyOf_1(Gen, Sub, Super).


%%	subPropertyOf(+Gen, ?Sub, ?Super) is nondet.
%
%	True when Node1 and Node2 are   connected, considering the graph
%	as an undirected graph.

subPropertyOf(Gen, Sub, Super) :-
	nonvar(Sub), nonvar(Super), !,
	bf_expand(subPropertyOf_1(Gen), Sub, [Super|_]), !.
subPropertyOf(Sub, Super, Gen) :-
	nonvar(Sub), !,
	bf_expand(subPropertyOf_1(Gen), Sub, [Super|_]).
subPropertyOf(Sub, Super, Gen) :-
	nonvar(Super), !,
	bf_expand(superPropertyOf_1(Gen), Super, [Sub|_]).
