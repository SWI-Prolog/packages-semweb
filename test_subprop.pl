:- module(test_subprop,
	  [ test/1,			% +Times
	    replay/0,
	    replay/1			% +File
	  ]).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(record)).
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
	snap/3,				% SnapID, Gen, Snap
	died/2.				% Born, Died

cleanup :-
	retractall(predicate(_,_)),
	retractall(sub_of(_,_,_)),
	retractall(died(_,_)),
	retractall(snap(_,_,_)),
	rdf_gc,
	rdf_reset_db.

test(N) :-
	record_in('g1.rec'),
	show_graph(g1),
	graph_settings(g1,
		       [ verify(0.01),
			 create_snap(0.1),
			 verify_snap(0.01)
		       ]),
	reset_graph(g1),
	loop(1, N).

loop(I, I) :- !.
loop(I, N) :-
	graph_steps(g1,1),
	succ(I, I2),
	format(user_error, '\r~t~D~6|', [I]),
	loop(I2, N).


:- listen(graph(g1, Action), update_graph_true(Action)).

update_graph_true(Action) :-
	update_graph(Action), !.
update_graph_true(_Action) :-
	assertion(false).

update_graph(Action) :-
	debug(subprop, '~p', [Action]),
	fail.
update_graph(reset) :-
	cleanup.
update_graph(verify) :-
	check_all.
update_graph(create_snap(SnapId)) :-
	rdf_snapshot(Snap),
	rdf_generation(Gen),
	assertz(snap(SnapId, Gen, Snap)).
update_graph(verify_snap(SnapId)) :-
	snap(SnapId, Gen, Snap),
	rdf_transaction(check_all(Gen), _Id, [snapshot(Snap)]).
update_graph(add_node(I)) :-
	atom_concat(p, I, P),
	rdf_statistics(triples(T0)),
	rdf_assert(P,P,P),
	rdf_statistics(triples(T1)),
	assertion(T0+1 =:= T1),
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
	forall(visible_predicate(Gen, P1),
	       forall(visible_predicate(Gen, P2),
		      check_all(Gen, P1, P2))).

check_all(Gen, Sub, Super) :-
	(   subPropertyOf(Gen, Sub, Super)
	->  assertion(rdf_has(Sub, Super, Sub))
	;   assertion(\+ rdf_has(Sub, Super, Sub))
	).



visible_predicate(Gen, P) :-
	predicate(P, Born),
	Gen >= Born,
	\+ (  died(Born, Died),
	      Gen >= Died
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


		 /*******************************
		 *	       RECORD		*
		 *******************************/

:- dynamic
	record_stream/1.

record_in(File) :-
	open(File, write, Out),
	asserta(record_stream(Out)),
	listen(record, graph(G, Action), save(G, Action)),
	at_halt(close_recording).

close_recording :-
	forall(retract(record_stream(Out)),
	       close(Out)).

save(Graph, Action) :-
	record_stream(Out),
	format(Out, 'action(~q, ~q).~n', [Graph, Action]),
	flush_output(Out).

%%	replay
%
%	Replay the last randomly generated suite

replay :-
	replay('g1.rec').

replay(File) :-
	open(File, read, In),
	repeat,
	    read(In, Term),
	    (	Term == end_of_file
	    ->	!, close(In)
	    ;	Term = action(Graph,Action),
		broadcast(graph(Graph, Action)),
		check_all,
		fail
	    ).
