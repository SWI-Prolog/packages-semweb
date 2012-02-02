:- module(rdf_test,
	  [ (+)/1,			% Assert
	    (-)/1,			% Retract
	    v/1,			% Visible
	    s/2,			% +Id, ?Subject
	    p/2,			% +Id, ?Predicate
	    o/2,			% +Id, ?Object
	    u/1,			% InVisible
	    l/0,			% List
	    r/0,			% reset
	    {}/1,			% transaction
	    (@)/2,			% Action @ Thread (Synchronous)
	    k/0,			% Kill helper threads
	    a/0,			% Run all tests
	    op(200, xfx, @)
	  ]).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(aggregate)).

/** <module> RDF test language
*/

:- meta_predicate
	true(0),
	false(0),
	{}(0),
	@(:,?).

:- thread_local
	triple/2.

%%	+ Pattern
%
%	Assert a triple, optionally giving it a name.

+ Name^{S,P,O} :- !,
	mk_spo(S,P,O),
	rdf_assert(S,P,O),
	(   var(Name)
	->  Name = rdf(S,P,O)
	;   assert(triple(Name, rdf(S,P,O)))
	).
+ {S,P,O} :-
	mk_spo(S,P,O),
	rdf_assert(S,P,O).

mk_spo(S,P,O) :-
	mk(s, S),
	mk(p, P),
	mk(o, O).

mk(_, R) :- atom(R), !.
mk(Prefix, R) :-
	gensym(Prefix, R).

%%	- Pattern
%
%	Retract a triple, normally referenced by name.

- Name^{S,P,O} :- !,
	rdf_retractall(S,P,O),
	(   var(Name)
	->  Name = rdf(S,P,O)
	;   assert(triple(Name, rdf(S,P,O)))
	).
- {S,P,O} :- !,
	rdf_retractall(S,P,O).
- rdf(S,P,O) :- !,
	rdf_retractall(S,P,O).
- Name :-
	ground(Name),
	triple(Name, Triple),
	-(Triple).

%%	v(+Id)
%
%	True if triple Id is visible.

v(rdf(S,P,O)) :- !,
	true((rdf(S,P,O))),
	true((rdf(S,P,O2), O == O2)),
	true((rdf(S,P2,O), P == P2)),
	true((rdf(S,P2,O2), P == P2, O == O2)),
	true((rdf(S2,P,O), S2 == S)),
	true((rdf(S2,P,O2), S2 == S, O == O2)),
	true((rdf(S2,P2,O), S2 == S, P == P2)),
	true((rdf(S2,P2,O2), S2 == S, P == P2, O == O2)).
v(Name) :-
	ground(Name),
	triple(Name, Triple),
	v(Triple).

%%	u(+Id)
%
%	True if triple Id is not visible.

u(rdf(S,P,O)) :- !,
	false((rdf(S,P,O))).
u(Name) :-
	ground(Name),
	triple(Name, Triple),
	u(Triple).

%%	s(Id, Subject) is semidet.
%%	p(Id, Predicate) is semidet.
%%	o(Id, Object) is semidet.

s(rdf(S,_,_), T) :- !,
	S = T.
s(Name, T) :-
	ground(Name),
	triple(Name, Triple),
	s(Triple, T).
p(rdf(_,P,_), T) :- !,
	P = T.
p(Name, T) :-
	ground(Name),
	triple(Name, Triple),
	p(Triple, T).
o(rdf(_,_,O), T) :- !,
	O = T.
o(Name, T) :-
	ground(Name),
	triple(Name, Triple),
	o(Triple, T).



true(G) :-
	G, !.
true(G) :-
	format(user_error, 'FALSE: ~q~n', [G]),
	backtrace(5),
	throw(test_failed).

false(G) :-
	G, !,
	format(user_error, 'TRUE: ~q~n', [G]),
	backtrace(5),
	throw(test_failed).
false(_).

%%	{G}
%
%	Run G in an RDF transaction.

{}(G) :-
	rdf_transaction(G).

%%	{G}@T
%
%	Run G (as once/1)  in  a  seperate   thread  and  wait  for  its
%	completion (synchronous execution).

:- dynamic
	helper/1.

(M:{}(G)) @ T :-
	(   var(T)
	->  thread_create(helper, T, []),
	    assert(helper(T))
	;   true
	),
	thread_self(Me),
	thread_send_message(T, run(M:G, Me)),
	thread_get_message(Reply),
	(   Reply = true(X)
	->  X = M:G
	;   Reply = exception(E)
	->  throw(E)
	).

%%	k
%
%	Kill all helper threads.

k :-
	forall(retract(helper(Id)),
	       (   thread_send_message(Id, done),
		   thread_join(Id, true)
	       )).

helper :-
	thread_get_message(M),
	(   M = run(G, Sender)
	->  run(G,Result),
	    thread_send_message(Sender, Result),
	    helper
	;   true
	).

run(G, Result) :-
	catch(G, E, true), !,
	(   var(E)
	->  Result = true(G)
	;   Result = exception(E)
	).
run(_, false).


%%	r
%
%	Reset the RDF database, helper threads, etc.

r :-
	k,
	retractall(triple(_,_)),
	rdf_reset_db.

%%	l
%
%	List content of RDF database.

l :-
	forall(rdf(S,P,O),
	       format('{~q, ~q, ~q}~n', [S,P,O])).


db(RDF) :-
	findall(rdf(S,P,O), rdf(S,P,O), RDF0),
	sort(RDF0, RDF).

		 /*******************************
		 *	       TESTS		*
		 *******************************/

:- op(1000, fx, test).

:- discontiguous (test)/1.

term_expansion((test Head :- Body),
	       [ test(Head),
		 (Head :- Body)
	       ]).

test t1 :-				% asserted triple in failed
	r,				% transaction disappears
	(  { + a^{_},
	     fail
	   }
	;  true
	),
	u(a).
test t2 :-				% asserted triple in transaction
	r,				% is visible inside and outside
	{ + a^{_},
	  v(a)
	},
	v(a).
test t3 :-
	r,
	{ + a^{_},
	  { v(a),
	    + b^{_},
	    v(b)
	  },
	  v(b)
	},
	v(a).
test t4 :-
	r,
	+ a^{_},
	{ v(a)
	}.
test t5 :-
	r,
	+ a^{_},
	{ - a,
	  u(a)
	},
	u(a).
test t6 :-
	r,
	+ a^{_},
	{ - a,
	  u(a)
	},
	u(a).
test t7 :-
	r,
	+ a^{_},
	(   { - a,
	      u(a),
	      fail
	    }
	;   true
	),
	v(a).
test p1 :-
	r,
	+ {s,p,_},
	+ B^{s,p,_},
	rdf(s,p,O),
	- B,
	o(B, O).
test p2 :-
	r,
	+ {s,p,_},
	+ B^{s,p,_},
	rdf(s,p,O),
	{- B} @ H,
	{u(B)} @ H,
	u(B),
	o(B, O).
test p3 :-
	r,
	+ B^{s,p,_},
	{-B}@_,
	u(B).

:- dynamic
	passed/1,
	failed/1.

%%	a
%
%	Run all tests

a :-
	retractall(passed(_)),
	retractall(failed(_)),
	forall(test(Head),
	       run(Head)),
	aggregate_all(count, passed(_), Passed),
	aggregate_all(count, failed(_), Failed),
	(   Failed =:= 0
	->  format('~NAll ~D tests passed~n', [Passed])
	;   format('~N~D tests passed; ~D failed~n', [Passed, Failed])
	).

run(Head) :-
	catch(Head, E, true), !,
	k,
	(   var(E)
	->  assert(passed(Head)),
	    write(user_error, '.')
	;   assert(failed(Head)),
	    format(user_error, '~NTEST FAILED: ~q~n', [Head])
	).
run(Head) :-
	k,
	assert(failed(Head)),
	format(user_error, '~NTEST FAILED: ~q~n', [Head]).
