:- module(random_graph,
	  [ action/1,
	    step/0,
	    steps/1,
	    show/0,
	    reset/0,
	    animate/0
	  ]).
:- use_module(pce_graph).
:- use_module(library(pce)).
:- use_module(library(debug)).
:- use_module(library(broadcast)).

/** <module>



*/

:- dynamic
	node/1,
	edge/2,				% Child, Parent
	node_id/1.

reset :-
	retractall(node(_)),
	retractall(edge(_,_)),
	retractall(node_id(_)),
	show(reset).

step :-
	findall(A-P, probability(A,P), Pairs),
	pairs_values(Pairs, Probs),
	sumlist(Probs, TotProb),
	repeat,
	   P is random_float*TotProb,
	   select_action(P, Pairs, Action),
	   action(Action), !,
	   debug(action, '~w', [Action]).

steps(N) :-
	succ(N2, N), !,
	step,
	steps(N2).
steps(_).

animate :-
	reset,
	show,
	repeat,
	   step,
	   in_pce_thread_sync(send(timer(0.2), delay)),
	   fail.



select_action(P, [A-PA|_], A) :-
	P < PA, !.
select_action(P, [_-PA|T], A) :-
	P2 is P-PA,
	select_action(P2, T, A).


probability(create_node,	      P) :-
	node_count(Nodes),
	edge_count(Edges),
	P is max(Edges,1)/max(10*Nodes,1).
probability(add_leaf,		      0.1).
probability(disconnect_leaf,	      0.1).
probability(connect_clouds,	      0.1).
probability(add_non_cyclic_edge,      0.1).
probability(add_cyclic_edge,	      0.03).
probability(remove_non_breaking_edge, 0.1).
probability(remove_edge,	      0.1).


action(create_node) :-
	next_node_id(Id),
	assert_node(Id).
action(add_leaf) :-
	unconnected_node(Node),
	leaf_node(Leaf), !,
	assert_edge(Node, Leaf).
action(disconnect_leaf) :-
	leaf_node(Leaf),
	retract_edge(Leaf, _Parent), !.
action(connect_clouds) :-
	clouds(Clouds),
	Clouds = [_,_|_],
	random_select(C1, Clouds, RestClouds),
	random_member(C2, RestClouds),
	random_member(N1, C1),
	random_member(N2, C2),
	assert_edge(N1, N2).
action(add_non_cyclic_edge) :-
	clouds(Clouds),
	random_permutation(Clouds, RC),
	member(C1, RC),
	random_permutation(C1, Perm),
	select(N1, Perm, R),
	member(N2, R),
	\+ creates_cycle(N1, N2), !,
	assert_edge(N1, N2).
action(add_cyclic_edge) :-
	clouds(Clouds),
	random_permutation(Clouds, RC),
	member(C1, RC),
	random_permutation(C1, Perm),
	select(N1, Perm, R),
	member(N2, R),
	creates_cycle(N1, N2), !,
	assert_edge(N1, N2).
action(remove_non_breaking_edge) :-
	edge(Child, Parent),
	connected(Parent, both_except(Child,Parent), Child), !,
	retract_edge(Child, Parent).
action(remove_edge) :-
	edge(Child, Parent), !,
	retract_edge(Child, Parent).


%%	assert_node(+Id) is det.
%%	assert_edge(+Child, +Parent) is det.
%%	retract_edge(+Child, +Parent) is nondet.

assert_node(Id) :-
	assertz(node(Id)),
	show(add_node(Id)).

assert_edge(Child, Parent) :-
	assert(edge(Child, Parent)),
	show(add_edge(Child, Parent)).

retract_edge(Child, Parent) :-
	retract(edge(Child, Parent)),
	show(del_edge(Child, Parent)).

%%	leaf_node(-Node) is nondet.

leaf_node(Leaf) :-
	edge(Leaf, _),
	\+ edge(_, Leaf).

%%	unconnected_node(-Node) is nondet.

unconnected_node(Node) :-
	node(Node),
	\+ edge(Node, _),
	\+ edge(_, Node).

%%	clouds(List) is det.
%
%	List of interconnected clouds

clouds(Clouds) :-
	findall(N, node(N), Nodes0),
	sort(Nodes0, Nodes),
	clouds(Nodes, Clouds).

clouds([], []).
clouds([H|T], [CH|CT]) :-
	findall(N, connected(H,both,N), NL),
	sort([H|NL], CH),
	ord_subtract(T, CH, RT),
	clouds(RT, CT).


%%	connected(?Node1, +Direction, ?Node2) is nondet.
%
%	True when Node1 and Node2 are   connected, considering the graph
%	as an undirected graph.

connected(N1, Dir, N2) :-
	nonvar(N1), nonvar(N2), !,
	connected([N1], [N1], Dir, N2), !.
connected(N1, Dir, N2) :-
	nonvar(N1), !,
	connected([N1], [N1], Dir, N2).
connected(N1, Dir, N2) :-
	nonvar(N2), !,
	revert_dir(Dir, RevDir),
	connected([N2], [N2], RevDir, N1).
connected(N1, Dir, N2) :-
	node(N1),
	connected([N1], [N1], Dir, N2).

connected([N|Agenda], Visited, Dir, Node) :-
	findall(New, connected_unvisited(N, Visited, Dir, New), NewList),
	(   member(Node, NewList)
	;   append(NewList, Visited, NewVisited),
	    append(Agenda, NewList, NewAgenda),
	    connected(NewAgenda, NewVisited, Dir, Node)
	).

connected_unvisited(N, Visited, both, New) :- !,
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


%%	creates_cycle(+Child, +Parent) is semidet.
%
%	True when adding Child-Parent to  the   graph,  the  graph would
%	become cyclic.

creates_cycle(Child, Parent) :-
	connected(Parent, forward, Child).

%%	next_node_id(-Id) is det.
%
%	Create a new unique node Id.

next_node_id(Id) :-
	retract(node_id(Id0)), !,
	Id is Id0+1,
	assertz(node_id(Id)).
next_node_id(1) :-
	assertz(node_id(1)).


node_count(Count) :-
	predicate_property(node(_), number_of_clauses(Count)).

edge_count(Count) :-
	predicate_property(edge(_,_), number_of_clauses(Count)).


		 /*******************************
		 *	       SHOW		*
		 *******************************/

:- pce_global(@graph, new(graph_viewer)).

show :-
	GV = @graph,
	send(GV, open),
	send(GV, generate, Child-Parent, random_graph:edge(Child, Parent)),
	forall(node(N),
	       get(GV, node, N, _Node)).

show(Action) :-
	broadcast(graph(Action)),
	(   object(@graph)
	->  show(Action, @graph)
	;   true
	).

show(add_node(N), GV) :-
	get(GV, node, N, _Node).
show(add_edge(C,P), GV) :-
	send(GV, display_arc, C, P).
show(del_edge(C,P), GV) :-
	send(GV, delete_arc, C, P).
show(reset, GV) :-
	send(GV, clear).
