:- module(test_subprop,
	  [ test/2			% +Size, +Steps
	  ]).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(record)).
:- use_module(library(settings)).
:- use_module(library(debug)).

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
	sub_of/3,			% P1, P2, Born
	died/2.				% Born, Died

cleanup :-
	retractall(sub_of(_,_,_)),
	retractall(died(_,_)).

%%	test(+Size, +Steps) is det.

test(Size, Steps) :-
	cleanup,
	make_state([size(Size)], State0),
	steps(Steps, State0, State),
	state_generation(State, Gen),
	dotty_graph(Gen).

steps(N, State0, State) :-
	N > 0, !,
	step(State0, State1),
	N1 is N - 1,
	steps(N1, State1, State).
steps(_, State, State).

sub_of(Sub, Super, Gen, Born) :-
	sub_of(Sub, Super, Born),
	Gen >= Born,
	\+ ( died(Born, Died),
	     Gen >= Died
	   ).

%%	is_sub_of(+Gen, +Sub, +Super) is semidet.

is_sub_of(Gen, Sub, Super) :-
	nonvar(Sub), nonvar(Super), !,
	is_sub_of(Gen, Sub, Super, []), !.
is_sub_of(Gen, Sub, Super) :-
	nonvar(Sub),
	is_sub_of(Gen, Sub, Super, []).

is_sub_of(_, Prop, Prop, _).
is_sub_of(Gen, Sub, Super, Visited) :-
	sub_of(Sub, Super1, Gen, _Born),
	\+ memberchk(Super1, Visited),
	is_sub_of(Gen, Super1, Super, [Super1|Visited]).


nth_sub_of(State, Nth, Sub, Super, Born) :-
	state_generation(State, Gen),
	nth0_answer(Nth, sub_of(Sub, Super, Gen, Born)).


add_sub_of(State0, Sub, Super, State) :-
	state_generation(State0, Gen0),
	state_count(State0, Count0),
	Gen is Gen0+1,
	Count is Count0+1,
	rdf_assert(Sub, rdfs:subPropertyOf, Super),
	assertz(sub_of(Sub, Super, Gen)),
	set_generation_of_state(Gen, State0, State1),
	set_count_of_state(Count, State1, State).

del_sub_of(State0, _Sub, _Super, Born, State) :-
	state_generation(State0, Gen0),
	state_count(State0, Count0),
	Gen is Gen0+1,
	Count is Count0-1,
	assert(died(Born, Gen)),
	set_generation_of_state(Gen, State0, State1),
	set_count_of_state(Count, State1, State).

%%	step(+State0, -State) is det.
%
%	Take a step in changing the subPropertyOf graph. Distinguish
%	operations:
%
%	  - Add operations
%	    - Extend a tree
%	    - Add non-cyclic link to tree
%	    - Add cyclic link to tree
%	    - Join two trees
%	  - Delete operations
%	    - Remove a leave
%	    - Break the tree
%	    - Remove a cycle
%	    - Remove a dummy non-cyclic link

step(State0, State) :-
	state_direction(State0, up), !,
	state_count(State0, Now),
	state_max_connections(State0, Max),
	(   Now > Max
	->  set_direction_of_state(down, State0, State1),
	    debug(subprop, 'Counting down', []),
	    step(State1, State)
	;   state_duplicates(State0, true)
	->  random_p(State0, Sub),
	    random_p(State0, Super),
	    add_sub_of(State0, Sub, Super, State)
	;   state_generation(State0, Gen),	% Avoid duplicates
	    repeat,
	    random_p(State0, Sub),
	    random_p(State0, Super),
	    \+ sub_of(Sub, Super, Gen, _Born)
	->  add_sub_of(State0, Sub, Super, State)
	).
step(State0, State) :-
	state_count(State0, Now),
	state_min_connections(State0, Min),
	(   Now =< Min
	->  set_direction_of_state(up, State0, State1),
	    debug(subprop, 'Counting up', []),
	    step(State1, State)
	;   Del is random(Now),
	    nth_sub_of(State0, Del, Sub, Super, Born),
	    del_sub_of(State0, Sub, Super, Born, State)
	).


random_p(State, P) :-
	state_size(State, Size),
	N is random(Size),		% 0..Size-1
	atomic_list_concat([p, N], P).


		 /*******************************
		 *	    GENERAL STUFF	*
		 *******************************/

nth0_answer(N, G) :-
	State = state(N),
	G,
	arg(1, State, N0),
	(   N0 == 0
	->  !
	;   N1 is N0-1,
	    nb_setarg(1, State, N1),
	    fail
	).


		 /*******************************
		 *	     SHOW GRAPH		*
		 *******************************/

:- setting(graphviz:dot_viewer, atom, xdot,
	   'Program to show dot graphs').

%%	dotty_graph(+Generation) is det.
%
%	Write dot representation to temporary file   and  open this file
%	using the dotty program.

dotty_graph(Gen) :-
	setup_call_cleanup(
	    tmp_file_stream(utf8, File, Out),
	    graph_to_dot(Out, Gen),
	    close(Out)),
%	process_create(path(cat), [File], []),
	setting(graphviz:dot_viewer, Program),
	thread_create(
	    run_dotty(Program, File),
	    _,
	    [detached(true)]).

:- dynamic
	dotty_process/1.

run_dotty(Program, File) :-
	process_create(path(Program), [File], [process(PID)]),
	assert(dotty_process(PID)),
	process_wait(PID, _),
	retractall(dotty_process(PID)).

kill_dotties :-
	forall(dotty_process(PID),
	       process_kill(PID)).

:- at_halt(kill_dotties).

graph_to_dot(Out, Gen) :-
	findall(Sub-Super, sub_of(Sub, Super, Gen, _Born), Pairs),
	phrase(graph(Pairs), Codes),
	format(Out, 'digraph xx {\n ~s}\n', [Codes]).

graph([]) --> [].
graph([H|T]) --> edge(H), graph(T).

edge(Sub-Super) -->
	gv_atom(Sub), " -> ", gv_atom(Super), ";\n".


gv_atom(A) -->
	{ atom_codes(A, Codes) },
	gv_string(Codes).

gv_string([]) --> [].
gv_string([H|T]) --> gv_string_code(H), gv_string(T).

%%	gv_string_code(+Code)// is det.
%
%	Emit (label) string.
%
%	@tbd	Complete definition, summarize long atoms, etc.

gv_string_code(32) --> !,
	"' ".
gv_string_code(0'\n) --> !,
	"'n".
gv_string_code(C) -->
	[C].

