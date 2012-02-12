:- module(test_subprop,
	  [ test/2			% +Size, +Steps
	  ]).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(record)).
:- use_module(library(settings)).
:- use_module(library(debug)).
:- use_module(library(broadcast)).
:- use_module(random_graph).

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

:- meta_predicate
	nth0_answer(+, 0).

:- record
	state(size:integer=20,
	      min_connections:integer=0,
	      max_connections:integer=40,
	      duplicates:boolean=false,
	      generation:integer=0,
	      count:integer=0,
	      direction=up).

:- dynamic
	generation/1,			% Current generation
	predicate/2,			% Predicate, Gen
	sub_of/3,			% P1, P2, Born
	died/2.				% Born, Died

cleanup :-
	retractall(generation(_)),
	retractall(predicate(_,_)),
	retractall(sub_of(_,_,_)),
	retractall(died(_,_)),
	assertz(generation(1)).

:- listen(graph(Action), update_graph(Action)).

update_graph(reset) :-
	cleanup.
update_graph(add_node(P)) :-
	rdf_assert(P,P,P),
	rdf_generation(Gen),
	assertz(predicate(P, Gen)).
update_graph(add_edge(Sub,Super)) :-
	rdf_assert(Sub, rdfs:subPropertyOf, Super),
	rdf_generation(Gen),
	assertz(sub_of(Sub,Super,Gen)).
update_graph(del_edge(Sub,Super)) :-
	rdf_retractall(Sub, rdfs:subPropertyOf, Super),
	rdf_generation(Gen),
	forall((sub_of(Sub,Super,Born),
		\+ died(Born, _)),
	       assertz(died(Born, Gen))).


subPropertyOf_1(Sub, Super) :-
	rdf_generation(Gen),
	subPropertyOf(Sub, Super, Gen).

subPropertyOf_1(Sub, Super, Gen) :-
	sub_of(Sub, Super, Born),
	Gen >= Born,
	\+ (  died(Born, Died),
	      Gen >= Died
	   ).

%%	subPropertyOf(?Sub, ?Super, +Gen) is nondet.
%
%	True when Node1 and Node2 are   connected, considering the graph
%	as an undirected graph.

subPropertyOf(Sub, Super, Gen) :-
	nonvar(Sub), nonvar(Super), !,
	connected([Sub], [Sub], Super, Gen), !.
subPropertyOf(Sub, Super, Gen) :-
	nonvar(Sub), !,
	connected([Sub], [Sub], Super, Gen).


connected([Sub|Agenda], Visited, Super, Gen) :-
	findall(New, connected_unvisited(Sub, Visited, New, Gen), NewList),
	(   member(Node, NewList)
	;   append(NewList, Visited, NewVisited),
	    append(Agenda, NewList, NewAgenda),
	    connected(NewAgenda, NewVisited, Node, Gen)
	).

connected_unvisited(N, Visited, New, Gen) :- !,
	(   edge(N, New)
	;   edge(New, N)
	),
	\+ memberchk(New, Visited).
connected_unvisited(N, Visited, forward, New) :- !,
	edge(N, New),
	\+ memberchk(New, Visited).
connected_unvisited(N, Visited, backward, New) :- !,
	edge(New, N),
	\+ memberchk(New, Visited).
connected_unvisited(N, Visited, both_except(Child,Parent), New) :- !,
	(   edge(N, New),
	    \+ (N == Child, New == Parent)
	;   edge(New, N),
	    \+ (New == Child, N == Parent)
	),
	\+ memberchk(New, Visited).

revert_dir(both,	     both).
revert_dir(both_except(C,P), both_except(P,C)).
revert_dir(forward,	     backward).
revert_dir(backward,	     forward).


