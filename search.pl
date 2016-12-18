:- module(search,
          [ bf_expand/3                 % :Edge, ?Src, -Path
          ]).
:- use_module(library(assoc)).
:- use_module(library(lists)).
:- use_module(library(error)).

:- meta_predicate
    bf_expand(2, +, -).

/** <module> Search library

This library provides implementations for various standard graph-search
algorithms.

*/

%!  bf_expand(:Edge, ?Src, -RevPath) is nondet.
%
%   Find a path  through  a  graph  for   which  the  edges  can  be
%   enumerated using call(Edge, From, To).
%
%   @param RevPath is the reversed path.  I.e., the head of this
%   path is the end of the path.  Using this argument order makes
%   it easier to find the target.

bf_expand(Edge, Src, Path) :-
    nonvar(Src),
    !,
    (   Path = [Src]
    ;   empty_assoc(Visited0),
        put_assoc(Src, Visited0, [[Src]], Visited),
        bf_expand(Edge, [[Src]|AT], AT, Visited, forward, Path)
    ).
bf_expand(_, Src, _) :-
    instantiation_error(Src).

bf_expand(_, Agenda, AT, _, _, _) :-
    Agenda == AT, !, fail.
bf_expand(Edge, [P0|Agenda], AT, Visited, Dir, Path) :-
    P0 = [Src|_],
    findall(New, connected(Dir, Edge, Src, New), NewList),
    into_visited(NewList, P0, Visited, NewVisited,
                 NewPaths,
                 NewList2, NewTail),
    (   NewTail = [],
        member(Path, NewList2)
    ;   member(Path, NewPaths)
    ;   AT = NewList2,
        NAT = NewTail,
        bf_expand(Edge, Agenda, NAT, NewVisited, Dir, Path)
    ).

connected(forward, Edge, Src, New) :-
    call(Edge, Src, New).
connected(backward, Edge, Src, New) :-
    call(Edge, New, Src).

into_visited([], _, Visited, Visited, [], T, T).
into_visited([H|R], Path, Visited0, Visited, [[H|Path]|NewPaths], T0, T) :-
    get_assoc(H,
              Visited0, Paths,
              Visited1, [[H|Path]|Paths]),
    !,
    into_visited(R, Path, Visited1, Visited, NewPaths, T0, T).
into_visited([H|R], Path, Visited0, Visited, NewPaths, [[H|Path]|T0], T) :-
    put_assoc(H, Visited0, [[H|Path]], Visited1),
    into_visited(R, Path, Visited1, Visited, NewPaths, T0, T).
