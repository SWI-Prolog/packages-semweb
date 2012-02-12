:- module(search,
	  [ bf_search/4,		% :Edge, ?Src, ?Dest, -Path
	    bf_search/5			% :Edge, ?Witness, ?Src, ?Dest, -Path
	  ]).
:- use_module(library(assoc)).
:- use_module(library(lists)).
:- use_module(library(error)).

:- meta_predicate
	bf_search(2, ?, ?, -),
	bf_search(2, ?, ?, ?, -).

/** <module> Search library

This library provides implementations for various standard graph-search
algorithms.

*/

%%	bf_search(:Edge, ?Src, ?Dest, -Path) is nondet.
%
%	Find a path  through  a  graph  for   which  the  edges  can  be
%	enumerated using call(Edge, From, To).

bf_search(Edge, Src, Dest, Path) :-
	nonvar(Src), !,
	empty_assoc(Visited0),
	put_assoc(Src, Visited0, true, Visited),
	bf_search(Edge, [Src|AT], AT, Visited, forward, Dest, Path).
bf_search(Edge, Src, Dest, Path) :-
	nonvar(Dest), !,
	empty_assoc(Visited0),
	put_assoc(Dest, Visited0, true, Visited),
	bf_search(Edge, [Dest|AT], AT, Visited, backward, Src, Path).
bf_search(_, Src, Dest, _) :-
	instantiation_error(Src-Dest).

bf_search(_, Agenda, AT, _, _, _, _) :-
	Agenda == AT, !, fail.
bf_search(Edge, [Src|Agenda], AT, Visited, Dir, Dest, Path) :-
	findall(New-Src, connected_unvisited(Dir, Edge, Src, Visited, New), NewList),
	into_visited(NewList, Visited, NewVisited, NewList2, []),
	(   Path = [Src,Dest],
	    member(Dest-Src, NewList)
	;   Path = [Src|Path2],
	    strip_w_agenda(NewList2, AT, NAT),
	    bf_search(Edge, Agenda, NAT, NewVisited, Dir, Dest, Path2)
	).

connected_unvisited(forward, Edge, Src, Visited, New) :-
	call(Edge, Src, New),
	\+ get_assoc(New, Visited, _).
connected_unvisited(backward, Edge, Src, Visited, New) :-
	call(Edge, New, Src),
	\+ get_assoc(New, Visited, _).

into_visited([], Visited, Visited, T, T).
into_visited([H|R], Visited0, Visited, T0, T) :-
	get_assoc(H, Visited0, true), !,
	into_visited(R, Visited0, Visited, T0, T).
into_visited([H|R], Visited0, Visited, [H|T0], T) :-
	put_assoc(H, Visited0, true, Visited1),
	into_visited(R, Visited1, Visited, T0, T).


%%	bf_search(:Edge, ?W, ?Src, ?Dest, -Path) is nondet.
%
%	Find a path  through  a  graph  for   which  the  edges  can  be
%	enumerated using call(Edge, From, To).

bf_search(Edge, W, Src, Dest, [n(Src)|Path]) :-
	nonvar(Src), !,
	empty_assoc(Visited0),
	put_assoc(Src, Visited0, true, Visited),
	bf_search(Edge, W, [Src|AT], AT, Visited, forward, Dest, Path).
bf_search(Edge, W, Src, Dest, [n(Src)|Path]) :-
	nonvar(Dest), !,
	empty_assoc(Visited0),
	put_assoc(Dest, Visited0, true, Visited),
	bf_search(Edge, W, [Dest|AT], AT, Visited, backward, Src, Path).
bf_search(_, _, Src, Dest, _) :-
	instantiation_error(Src-Dest).

bf_search(_, _, Agenda, AT, _, _, _, _) :-
	Agenda == AT, !, fail.
bf_search(Edge, W, [Src|Agenda], AT, Visited, Dir, Dest, Path) :-
	findall(New-W, connected_unvisited(Dir, Edge, Src, Visited, New), NewList),
	into_visited(NewList, Visited, NewVisited, NewList2, []),
	(   Path = [w(W), n(Dest)],
	    member(Dest-W, NewList)
	;   AT = NewList2,
	    Path = [New|Path2],
	    strip_w_agenda(NewList2, AT, NAT),
	    bf_search(Edge, Agenda, NAT, NewVisited, Dir, Dest, Path2)
	).

strip_w_agenda([], NAT, NAT).
strip_w_agenda([N-_|R], [N|T0], T) :-
	strip_w_agenda(R, T0, T).
