:- module(random_graph,
          [ graph_steps/2,              % +Graph, +Steps
            graph_settings/2,           % +Graph, +List
            graph_action/2,             % +Graph, +Action
            show_graph/1,               % +Graph
            reset_graph/1,              % +Graph
            animate_graph/3             % +Graph, +Steps, +Delay
          ]).
:- if(exists_source(library(pce))).
:- use_module(pce_graph).
:- use_module(library(pce)).
:- endif.
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(random)).
:- use_module(library(broadcast)).

/** <module>

This library creates randomly changing graphs.
*/

:- dynamic
    node/2,                         % Id, Graph
    edge/3,                         % Child, Parent, Graph
    snap/2,                         % Graph, Id
    id/3,                           % Id, Type, Graph
    probability/3.                  % Graph, Action, Prob

%!  reset_graph(+Graph)
%
%   Remove all information about Graph.

reset_graph(Graph) :-
    retractall(node(_, Graph)),
    retractall(edge(_,_, Graph)),
    retractall(snap(Graph, _)),
    retractall(id(Graph, _, _)),
    show(Graph, reset).

%!  graph_steps(+Graph, +Steps) is det.
%
%   Perform N random actions on Graph.

graph_steps(Graph, N) :-
    succ(N2, N),
    !,
    step(Graph),
    graph_steps(Graph, N2).
graph_steps(_, _).

step(Graph) :-
    findall(A-P, action_probability(Graph,A,P), Pairs),
    pairs_values(Pairs, Probs),
    sum_list(Probs, TotProb),
    repeat,
       P is random_float*TotProb,
       select_action(P, Pairs, Action),
       graph_action(Graph, Action),
       !,
       debug(graph_action, '~w', [Action]).

%!  animate_graph(+Graph, +Steps, +Delay)
%
%   Show an animated graph, performing N steps with Delay seconds
%   between the steps.

animate_graph(Graph, Steps, Delay) :-
    reset_graph(Graph),
    show_graph(Graph),
    forall(between(1, Steps, _),
           ( step(Graph),
             sleep(Delay)
           )).

select_action(P, [A-PA|_], A) :-
    P < PA,
    !.
select_action(P, [_-PA|T], A) :-
    P2 is P-PA,
    select_action(P2, T, A).

%!  graph_settings(+Graph, +List) is det.
%
%   Add settings for the graph.

graph_settings(Graph, List) :-
    must_be(list, List),
    maplist(graph_setting(Graph), List).

graph_setting(Graph, Prop) :-
    Prop =.. [Action,Prob],
    (   action(Action)
    ->  retractall(probability(Graph, Action, _)),
        asserta(probability(Graph, Action, Prob))
    ;   existence_error(action, Prob)
    ).


action_probability(Graph, Action, P) :-
    (   var(Action)
    ->  action(Action)
    ;   true
    ),
    (   probability(Graph, Action, P)
    ->  true
    ;   default_probability(Graph, Action, P)
    ).

action(Action) :-
    clause(graph_action(_, Action), _).

default_probability(Graph, create_node, P) :-
    node_count(Graph, Nodes),
    edge_count(Graph, Edges),
    P is max(Edges,1)/max(10*Nodes,1).
default_probability(_, add_leaf,                 0.1).
default_probability(_, disconnect_leaf,          0.1).
default_probability(_, connect_clouds,           0.1).
default_probability(_, add_non_cyclic_edge,      0.1).
default_probability(_, add_cyclic_edge,          0.03).
default_probability(_, remove_non_breaking_edge, 0.1).
default_probability(_, remove_edge,              0.1).


%!  graph_action(+Graph, +Action) is det.
%
%   Perform Action on Graph.

graph_action(Graph, create_node) :-
    next_id(Graph, node, Id),
    assert_node(Graph, Id).
graph_action(Graph, add_leaf) :-
    unconnected_node(Graph, Node),
    leaf_node(Graph, Leaf),
    !,
    assert_edge(Graph, Node, Leaf).
graph_action(Graph, disconnect_leaf) :-
    leaf_node(Graph, Leaf),
    retract_edge(Graph, Leaf, _Parent),
    !.
graph_action(Graph, connect_clouds) :-
    clouds(Graph, Clouds),
    Clouds = [_,_|_],
    random_select(C1, Clouds, RestClouds),
    random_member(C2, RestClouds),
    random_member(N1, C1),
    random_member(N2, C2),
    assert_edge(Graph, N1, N2).
graph_action(Graph, add_non_cyclic_edge) :-
    clouds(Graph, Clouds),
    random_permutation(Clouds, RC),
    member(C1, RC),
    random_permutation(C1, Perm),
    select(N1, Perm, R),
    member(N2, R),
    \+ creates_cycle(Graph, N1, N2),
    !,
    assert_edge(Graph, N1, N2).
graph_action(Graph, add_cyclic_edge) :-
    clouds(Graph, Clouds),
    random_permutation(Clouds, RC),
    member(C1, RC),
    random_permutation(C1, Perm),
    select(N1, Perm, R),
    member(N2, R),
    creates_cycle(Graph, N1, N2),
    !,
    assert_edge(Graph, N1, N2).
graph_action(Graph, remove_non_breaking_edge) :-
    edge(Child, Parent, Graph),
    connected(Graph, Parent, both_except(Child,Parent), Child),
    !,
    retract_edge(Graph, Child, Parent).
graph_action(Graph, remove_edge) :-
    edge(Child, Parent, Graph),
    !,
    retract_edge(Graph, Child, Parent).
graph_action(Graph, verify) :-
    show(Graph, verify).
graph_action(Graph, create_snap) :-
    next_id(Graph, snap, SnapID),
    assertz(snap(Graph, SnapID)),
    show(Graph, create_snap(SnapID)).
graph_action(Graph, verify_snap) :-
    findall(SnapID, snap(Graph, SnapID), Candidates),
    random_member(VerifyID, Candidates),
    show(Graph, verify_snap(VerifyID)).
graph_action(Graph, delete_snap) :-     % delete snap, preferably an old one
    (   snap(Graph,_)
    ->  (   repeat,
            clause(snap(Graph, SnapID), true, Ref),
            maybe,
            erase(Ref)
        ->  show(Graph, delete_snap(SnapID))
        ;   true
        )
    ;   true
    ).


%!  assert_node(+Graph, +Id) is det.
%!  assert_edge(+Child, +Parent) is det.
%!  retract_edge(+Child, +Parent) is nondet.

assert_node(Graph, Id) :-
    assertz(node(Id, Graph)),
    show(Graph, add_node(Id)).

assert_edge(Graph, Child, Parent) :-
    edge(Child, Parent, Graph),
    !.
assert_edge(Graph, Child, Parent) :-
    assert(edge(Child, Parent, Graph)),
    show(Graph, add_edge(Child, Parent)).

retract_edge(Graph, Child, Parent) :-
    retract(edge(Child, Parent, Graph)),
    show(Graph, del_edge(Child, Parent)).

%!  leaf_node(-Node) is nondet.

leaf_node(Graph, Leaf) :-
    edge(Leaf, _, Graph),
    \+ edge(_, Leaf, Graph).

%!  unconnected_node(-Node) is nondet.

unconnected_node(Graph, Node) :-
    node(Node, Graph),
    \+ edge(Node, _, Graph),
    \+ edge(_, Node, Graph).

%!  clouds(Graph, List) is det.
%
%   List of interconnected clouds

clouds(Graph, Clouds) :-
    findall(N, node(N, Graph), Nodes0),
    sort(Nodes0, Nodes),
    clouds(Nodes, Clouds, Graph).

clouds([], [], _).
clouds([H|T], [CH|CT], Graph) :-
    findall(N, connected(Graph,H,both,N), NL),
    sort([H|NL], CH),
    ord_subtract(T, CH, RT),
    clouds(RT, CT, Graph).


%!  connected(+Graph, ?Node1, +Direction, ?Node2) is nondet.
%
%   True when Node1 and Node2 are   connected, considering the graph
%   as an undirected graph.

connected(Graph, N1, Dir, N2) :-
    nonvar(N1), nonvar(N2),
    !,
    connected([N1], [N1], Dir, N2, Graph),
    !.
connected(Graph, N1, Dir, N2) :-
    nonvar(N1),
    !,
    connected([N1], [N1], Dir, N2, Graph).
connected(Graph, N1, Dir, N2) :-
    nonvar(N2),
    !,
    revert_dir(Dir, RevDir),
    connected([N2], [N2], RevDir, N1, Graph).
connected(Graph, N1, Dir, N2) :-
    node(N1, Graph),
    connected([N1], [N1], Dir, N2, Graph).

connected([N|Agenda], Visited, Dir, Node, Graph) :-
    findall(New, connected_unvisited(N, Visited, Dir, New, Graph), NewList),
    (   member(Node, NewList)
    ;   append(NewList, Visited, NewVisited),
        append(Agenda, NewList, NewAgenda),
        connected(NewAgenda, NewVisited, Dir, Node, Graph)
    ).

connected_unvisited(N, Visited, both, New, Graph) :-
    !,
    (   edge(N, New, Graph)
    ;   edge(New, N, Graph)
    ),
    \+ memberchk(New, Visited).
connected_unvisited(N, Visited, forward, New, Graph) :-
    !,
    edge(N, New, Graph),
    \+ memberchk(New, Visited).
connected_unvisited(N, Visited, backward, New, Graph) :-
    !,
    edge(New, N, Graph),
    \+ memberchk(New, Visited).
connected_unvisited(N, Visited, both_except(Child,Parent), New, Graph) :-
    !,
    (   edge(N, New, Graph),
        \+ (N == Child, New == Parent)
    ;   edge(New, N, Graph),
        \+ (New == Child, N == Parent)
    ),
    \+ memberchk(New, Visited).

revert_dir(both,             both).
revert_dir(both_except(C,P), both_except(P,C)).
revert_dir(forward,          backward).
revert_dir(backward,         forward).


%!  creates_cycle(+Graph, +Child, +Parent) is semidet.
%
%   True when adding Child-Parent to  the   graph,  the  graph would
%   become cyclic.

creates_cycle(Graph, Child, Parent) :-
    connected(Graph, Parent, forward, Child).

%!  next_id(+Graph, +Type, -Id) is det.
%
%   Create a new unique node Id.

next_id(Graph, Type, Id) :-
    retract(id(Graph, Type, Id0)),
    !,
    Id is Id0+1,
    assertz(id(Graph, Type, Id)).
next_id(Graph, Type, 1) :-
    assertz(id(Graph, Type, 1)).


node_count(Graph, Count) :-
    aggregate_all(count, node(_,Graph), Count).

edge_count(Graph, Count) :-
    aggregate_all(count, edge(_,_,Graph), Count).


                 /*******************************
                 *             SHOW             *
                 *******************************/

show(Graph, Action) :-
    broadcast(graph(Graph, Action)).


:- if(exists_source(library(pce))).
:- pce_global(@(graph), new(graph_viewer)).

%!  show_graph(+Graph) is det.
%
%   Create a graphics window for Graph.  The window listens for
%   broadcasts messages, updating if the graph changes.

show_graph(Graph) :-
    in_pce_thread_sync(broadcast_request(graph(Graph))),
    !.
show_graph(Graph) :-
    in_pce_thread_sync(pce_show_graph(Graph)).

pce_show_graph(Graph) :-
    context_module(Here),
    new(GV, graph_viewer(Graph)),
    send(GV, open),
    send(GV, generate, Child-Parent,
         Here:edge(Child, Parent, Graph)),
    forall(node(N, Graph),
           get(GV, node, N, _Node)).

:- else.

show_graph(_) :-
    format(user_error, 'Cannot show graphs without xpce~n', []).

:- endif.
