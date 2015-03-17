/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2006-2015, University of Amsterdam
			      VU University Amsterdam

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

    As a special exception, if you link this library with other files,
    compiled with a Free Software compiler, to produce an executable, this
    library does not by itself cause the resulting executable to be covered
    by the GNU General Public License. This exception does not however
    invalidate any other reasons why the executable file might be covered by
    the GNU General Public License.
*/


:- module(rdf_litindex,
	  [ rdf_set_literal_index_option/1,	% +Options
	    rdf_tokenize_literal/2,		% +Literal, -Tokens
	    rdf_find_literal/2,			% +Spec, -Literal
	    rdf_find_literals/2,		% +Spec, -ListOfLiterals
	    rdf_token_expansions/2,		% +Spec, -Expansions

	    rdf_literal_index/2			% +Type, -Index
	  ]).
:- use_module(rdf_db).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(error)).
:- use_module(library(apply)).
:- if(exists_source(library(snowball))).
:- use_module(library(snowball)).
:- else.
:- use_module(library(porter_stem)).
:- endif.
:- use_module(library(double_metaphone)).

/** <module> Search literals

This module finds literals of the RDF  database based on words, stemming
and _sounds like_ (metaphone).  The normal user-level predicate is

  - rdf_find_literals/2
*/

:- dynamic
	literal_map/2,			% Type, -Map
	map_building/2,			% Type, -Queue
	new_token/2,			% Hook
	setting/1.
:- volatile
	literal_map/2.
:- multifile
	tokenization/2,			% +Literal, -Tokens
	exclude_from_index/2.		% +Which, +Token


setting(verbose(false)).		% print progress messages
setting(index_threads(1)).		% # threads for creating the index
setting(index(thread(1))).		% Use a thread for incremental updates

%%	rdf_set_literal_index_option(+Options:list)
%
%	Set options for the literal package.  Currently defined options
%
%		* verbose(Bool)
%		If =true=, print progress messages while building the
%		index tables.
%
%		* index_threads(+Count)
%		Number of threads to use for initial indexing of
%		literals
%
%		* index(+How)
%		How to deal with indexing new literals.  How is one of
%		=self= (execute in the same thread), thread(N) (execute
%		in N concurrent threads) or =default= (depends on number
%		of cores).

rdf_set_literal_index_option([]) :- !.
rdf_set_literal_index_option([H|T]) :- !,
	set_option(H),
	rdf_set_literal_index_option(T).
rdf_set_literal_index_option(Option) :-
	set_option(Option).

set_option(Term) :-
	check_option(Term),
	functor(Term, Name, Arity),
	functor(General, Name, Arity),
	retractall(setting(General)),
	assert(setting(Term)).

check_option(X) :-
	var(X), !,
	instantiation_error(X).
check_option(verbose(X)) :- !,
	must_be(boolean, X).
check_option(index_threads(Count)) :- !,
	must_be(nonneg, Count).
check_option(index(How)) :- !,
	must_be(oneof([default,thread(_),self]), How).
check_option(Option) :-
	domain_error(literal_option, Option).


		 /*******************************
		 *	      QUERY		*
		 *******************************/

%%	rdf_find_literal(+Spec, -Literal) is nondet.
%%	rdf_find_literals(+Spec, -Literals) is det.
%
%	Find literals in the RDF database matching Spec.  Spec is defined
%	as:
%
%	==
%	Spec ::= and(Spec,Spec)
%	Spec ::= or(Spec,Spec)
%	Spec ::= not(Spec)
%	Spec ::= sounds(Like)
%	Spec ::= stem(Like)		% same as stem(Like, en)
%	Spec ::= stem(Like, Lang)
%	Spec ::= prefix(Prefix)
%	Spec ::= between(Low, High)	% Numerical between
%	Spec ::= ge(High)		% Numerical greater-equal
%	Spec ::= le(Low)		% Numerical less-equal
%	Spec ::= Token
%	==
%
%	sounds(Like) and stem(Like) both map to  a disjunction. First we
%	compile the spec to normal form:   a disjunction of conjunctions
%	on elementary tokens. Then we execute   all the conjunctions and
%	generate the union using ordered-set algorithms.
%
%	@tbd Exploit ordering of numbers and allow for > N, < N, etc.

rdf_find_literal(Spec, Literal) :-
	rdf_find_literals(Spec, Literals),
	member(Literal, Literals).

rdf_find_literals(Spec, Literals) :-
	compile_spec(Spec, DNF),
	token_index(Map),
	lookup(DNF, Map, _, SuperSet),
	flatten(SuperSet, Set0),
	sort(Set0, Literals).

%%	rdf_token_expansions(+Spec, -Extensions)
%
%	Determine which extensions of  a   token  contribute  to finding
%	literals.

rdf_token_expansions(prefix(Prefix), [prefix(Prefix, Tokens)]) :-
	token_index(Map),
	rdf_keys_in_literal_map(Map, prefix(Prefix), Tokens).
rdf_token_expansions(sounds(Like), [sounds(Like, Tokens)]) :-
	metaphone_index(Map),
	rdf_find_literal_map(Map, [Like], Tokens).
rdf_token_expansions(stem(Like), [stem(Like, Tokens)]) :-
	stem_index(Map),
	rdf_find_literal_map(Map, [Like], Tokens).
rdf_token_expansions(Spec, Expansions) :-
	compile_spec(Spec, DNF),
	token_index(Map),
	lookup(DNF, Map, SCS, _),
	flatten(SCS, CS),
	sort(CS, Expansions0),
	join_expansions(Expansions0, Expansions).

join_expansions([], []).
join_expansions([H0|T0], [H|T]) :-
	untag(H0, Tag, V0),
	Tag =.. L0,
	append(L0, [[V0|Values]], L1),
	H =.. L1,
	join_expansions_by_tag(T0, Tag, T1, Values),
	join_expansions(T1, T).

join_expansions_by_tag([H|T0], Tag, T, [V0|VT]) :-
	untag(H, Tag, V0), !,
	join_expansions_by_tag(T0, Tag, T, VT).
join_expansions_by_tag(L, _, L, []).

lookup(@(false), _, [], []) :- !.
lookup(or(H0,T0), Map, [CH|CT], [H|T]) :- !,
	lookup(H0, Map, CH, H),
	lookup(T0, Map, CT, T).
lookup(H0, Map, [C], [H]) :-
	lookup1(H0, Map, C, H).

lookup1(Conj, Map, Cond, Literals) :-
	phrase(conj_to_list(Conj), List), !,
	rdf_find_literal_map(Map, List, Literals),
	(   Literals \== []
	->  phrase(conj_to_cond(Conj), Cond)
	;   Cond = []
	).
lookup1(_, _, _, []).

conj_to_list(and(A,B)) --> !,
	conj_to_list(A),
	conj_to_list(B).
conj_to_list(@(false)) --> !,
	{fail}.
conj_to_list(Tagged) -->
	{ untag(Tagged, L) }, !,
	[L].
conj_to_list(L) -->
	[L].


conj_to_cond(and(A,B)) --> !,
	conj_to_cond(A),
	conj_to_cond(B).
conj_to_cond(Tagged) -->
	{ untag(Tagged, _) }, !,
	[ Tagged ].
conj_to_cond(_) -->
	[].


%%	compile_spec(+Spec, -Compiled)
%
%	Compile a specification as above into disjunctive normal form

compile_spec(Spec, DNF) :-
	expand_fuzzy(Spec, Spec2),
	nnf(Spec2, NNF),
	dnf(NNF, DNF).


expand_fuzzy(Var, _) :-
	var(Var), !,
	throw(error(instantiation_error, _)).
expand_fuzzy(sounds(Like), Or) :- !,
	(   atom(Like)
	->  metaphone_index(Map),
	    double_metaphone(Like, Key),
	    rdf_find_literal_map(Map, [Key], Tokens),
	    list_to_or(Tokens, sounds(Like), Or)
	;   expand_fuzzy(Like, Or)
	).
expand_fuzzy(stem(Like), Or) :- !,
	expand_fuzzy(stem(Like, en), Or).
expand_fuzzy(stem(Like, Lang), Or) :- !,
	(   atom(Like)
	->  stem_index(Map),
	    stem(Like, Lang, Key),
	    rdf_find_literal_map(Map, [Key], Tokens),
	    list_to_or(Tokens, stem(Like), Or)
	;   expand_fuzzy(Like, Or)
	).
expand_fuzzy(prefix(Prefix), Or) :- !,
	(   atom(Prefix)
	->  token_index(Map),
	    rdf_keys_in_literal_map(Map, prefix(Prefix), Tokens),
	    list_to_or(Tokens, prefix(Prefix), Or)
	;   expand_fuzzy(Prefix, Or)
	).
expand_fuzzy(case(String), Or) :- !,
	(   atom(String)
	->  token_index(Map),
	    rdf_keys_in_literal_map(Map, case(String), Tokens),
	    list_to_or(Tokens, case(String), Or)
	;   expand_fuzzy(String, Or)
	).
expand_fuzzy(or(A0, B0), E) :- !,
	expand_fuzzy(A0, A),
	expand_fuzzy(B0, B),
	simplify(or(A,B), E).
expand_fuzzy(and(A0, B0), E) :- !,
	expand_fuzzy(A0, A),
	expand_fuzzy(B0, B),
	simplify(and(A,B), E).
expand_fuzzy(not(A0), not(A)) :- !,
	expand_fuzzy(A0, A).
expand_fuzzy(between(Low, High), Or) :- !,
	token_index(Map),
	rdf_keys_in_literal_map(Map, between(Low, High), Tokens),
	list_to_or(Tokens, between(Low, High), Or).
expand_fuzzy(le(High), Or) :- !,
	token_index(Map),
	rdf_keys_in_literal_map(Map, le(High), Tokens),
	list_to_or(Tokens, le(High), Or).
expand_fuzzy(ge(Low), Or) :- !,
	token_index(Map),
	rdf_keys_in_literal_map(Map, ge(Low), Tokens),
	list_to_or(Tokens, ge(Low), Or).
expand_fuzzy(Token, Token) :-
	atomic(Token), !.
expand_fuzzy(Token, _) :-
	throw(error(type_error(Token, boolean_expression), _)).

simplify(Expr0, Expr) :-
	simple(Expr0, Expr), !.
simplify(Expr, Expr).

simple(and(@(false), _), @(false)).
simple(and(_, @(false)), @(false)).
simple(or(@(false), X), X).
simple(or(X, @(false)), X).


list_to_or([], _, @(false)) :- !.
list_to_or([X], How, One) :- !,
	tag(How, X, One).
list_to_or([H0|T0], How, or(H, T)) :-
	tag(How, H0, H),
	list_to_or(T0, How, T).

tag(sounds(X),	  Y, sounds(X,Y)).
tag(stem(X),	  Y, stem(X,Y)).
tag(prefix(X),	  Y, prefix(X,Y)).
tag(case(X),	  Y, case(X,Y)).
tag(between(L,H), Y, between(L,H,Y)).
tag(ge(L),	  Y, ge(L,Y)).
tag(le(H),	  Y, le(H,Y)).

untag(sounds(_,Y),    Y).
untag(stem(_,Y),      Y).
untag(prefix(_,Y),    Y).
untag(case(_,Y),      Y).
untag(between(_,_,Y), Y).
untag(le(_,Y),	      Y).
untag(ge(_,Y),	      Y).

untag(sounds(X,Y),    sounds(X),    Y).
untag(stem(X,Y),      stem(X),	    Y).
untag(prefix(X,Y),    prefix(X),    Y).
untag(case(X,Y),      case(X),	    Y).
untag(between(L,H,Y), between(L,H), Y).
untag(ge(L,Y),	      ge(L),	    Y).
untag(le(H,Y),	      le(H),	    Y).


%%	nnf(+Formula, -NNF)
%
%	Rewrite to Negative Normal Form, meaning negations only appear
%	around literals.

nnf(not(not(A0)), A) :- !,
	nnf(A0, A).
nnf(not(and(A0,B0)), or(A,B)) :- !,
	nnf(not(A0), A),
	nnf(not(B0), B).
nnf(not(or(A0,B0)), and(A,B)) :- !,
	nnf(not(A0), A),
	nnf(not(B0), B).
nnf(A, A).


%%	dnf(+NNF, -DNF)
%
%	Convert a formula in NNF to Disjunctive Normal Form (DNF)

dnf(or(A0,B0), or(A, B)) :- !,
	dnf(A0, A),
	dnf(B0, B).
dnf(and(A0,B0), DNF):- !,
	dnf(A0, A1),
	dnf(B0, B1),
	dnf1(and(A1,B1), DNF).
dnf(DNF, DNF).

dnf1(and(A0, or(B,C)), or(P,Q)) :- !,
	dnf1(and(A0,B), P),
	dnf1(and(A0,C), Q).
dnf1(and(or(B,C), A0), or(P,Q)) :- !,
	dnf1(and(A0,B), P),
	dnf1(and(A0,C), Q).
dnf1(DNF, DNF).


		 /*******************************
		 *	    TOKEN INDEX		*
		 *******************************/

%%	token_index(-Map)
%
%	Get the index of tokens. If  not   present,  create one from the
%	current database. Once created, the map is kept up-to-date using
%	a monitor hook.

token_index(Map) :-
	literal_map(token, Map), !,
	wait_for_map(token).
token_index(Map) :-
	rdf_new_literal_map(Map),
	assert(literal_map(token, Map)),
	register_token_updater,
	message_queue_create(Queue),
	assert(map_building(token, Queue)),
	thread_create(make_literal_index(Queue), _,
		      [ alias('__rdf_tokenizer'),
			detached(true)
		      ]),
	wait_for_map(token).

register_token_updater :-
	Monitor = [ reset,
		    new_literal,
		    old_literal
		  ],
	(   setting(index(default))
	->  create_update_literal_thread(1),
	    rdf_monitor(thread_monitor_literal, Monitor)
	;   setting(index(thread(N)))
	->  create_update_literal_thread(N),
	    rdf_monitor(thread_monitor_literal, Monitor)
	;   rdf_monitor(monitor_literal, Monitor)
	).

make_literal_index(Queue) :-
	call_cleanup(
	    make_literal_index,
	    ( message_queue_destroy(Queue),
	      retractall(map_building(token, _)))).

%%	make_literal_index
%
%	Create the initial literal index.

make_literal_index :-
	setting(index_threads(N)), !,
	threaded_literal_index(N),
	verbose('~N', []).
make_literal_index :-
	current_prolog_flag(cpu_count, X),
	threaded_literal_index(X),
	verbose('~N', []).

threaded_literal_index(N) :-
	N > 1, !,
	message_queue_create(Q, [max_size(1000)]),
	create_index_threads(N, Q, Ids),
	forall(rdf_current_literal(Literal),
	       thread_send_message(Q, Literal)),
	forall(between(1, N, _),
	       thread_send_message(Q, done(true))),
	maplist(thread_join, Ids, _).
threaded_literal_index(_) :-
	forall(rdf_current_literal(Literal),
	       register_literal(Literal)).

create_index_threads(N, Q, [Id|T]) :-
	N > 0, !,
	thread_create(index_worker(Q), Id, []),
	N2 is N - 1,
	create_index_threads(N2, Q, T).
create_index_threads(_, _, []) :- !.

index_worker(Queue) :-
	repeat,
	    thread_get_message(Queue, Msg),
	    work(Msg).

work(done(true)) :- !.
work(Literal) :-
	register_literal(Literal),
	fail.


%	clean_token_index
%
%	Clean after a reset.

clean_token_index :-
	forall(literal_map(_, Map),
	       rdf_reset_literal_map(Map)).

		 /*******************************
		 *	  THREADED UPDATE	*
		 *******************************/

%%	create_update_literal_thread(+Threads)
%
%	Setup literal monitoring using threads.  While loading databases
%	through rdf_attach_db/2 from  rdf_persistency.pl,   most  of the
%	time is spent updating the literal token database. While loading
%	the RDF triples, most of the time   is spend in updating the AVL
%	tree holding the literals. Updating  the   token  index hangs on
%	updating the AVL trees holding the   tokens.  Both tasks however
%	can run concurrently.

create_update_literal_thread(Threads) :-
	message_queue_create(_,
			     [ alias(rdf_literal_monitor_queue),
			       max_size(50000)
			     ]),
	forall(between(1, Threads, _),
	       create_index_worker(initial)).

:- dynamic
	index_worker_id/1,
	extra_worker_count/1.

create_index_worker(Status) :-
	(   retract(index_worker_id(Id0))
	->  true
	;   Id0 = 1
	),
	succ(Id0, Id1),
	assertz(index_worker_id(Id1)),
	atom_concat(rdf_literal_monitor_, Id0, Alias),
	inc_extra_worker_count(Status),
	thread_create(monitor_literals(Status), _,
		      [ alias(Alias)
		      ]).

monitor_literals(initial) :-
	set_prolog_flag(agc_margin, 0),	% we don't create garbage
	repeat,
	    thread_get_message(rdf_literal_monitor_queue, Literal),
	    register_literal(Literal),
	fail.
monitor_literals(extra) :-
	set_prolog_flag(agc_margin, 0),
	repeat,
	    (	thread_get_message(rdf_literal_monitor_queue, Literal,
				   [ timeout(1)
				   ])
	    ->	register_literal(Literal),
		fail
	    ;	!
	    ),
	with_mutex(create_index_worker, dec_extra_worker_count),
	thread_self(Me),
	thread_detach(Me).

thread_monitor_literal(new_literal(Literal)) :- !,
	thread_send_message(rdf_literal_monitor_queue, Literal).
thread_monitor_literal(Action) :- !,
	monitor_literal(Action).

%%	check_index_workers(+Queue, +Keys)
%
%	Increase the number of workers indexing   literals sent to Queue
%	if the queue gets overful.

check_index_workers(Alias, Keys) :-
	max_extra_workers(Max),
	Max > 0,
	message_queue_property(Queue, alias(Alias)),
	message_queue_property(Queue, size(Size)),
	Size > 10000,
	\+ ( extra_worker_count(Extra),
	     Extra >= Max
	   ), !,
	debug(rdf_litindex,
	      'Creating extra literal indexer (Queue=~D, Keys=~D)',
	      [Size, Keys]),
	with_mutex(create_index_worker, create_index_worker(extra)).
check_index_workers(_, _).

inc_extra_worker_count(extra) :- !,
	(   retract(extra_worker_count(C0))
	->  C is C0+1
	;   C = 1
	),
	asserta(extra_worker_count(C)).
inc_extra_worker_count(_).

dec_extra_worker_count :-
	retract(extra_worker_count(C0)), !,
	C is C0-1,
	asserta(extra_worker_count(C)).
dec_extra_worker_count.

max_extra_workers(Max) :-
	current_prolog_flag(cpu_count, Count),
	Max is Count//2.


		 /*******************************
		 *	 MONITORED UPDATE	*
		 *******************************/

monitor_literal(new_literal(Literal)) :-
	register_literal(Literal).
monitor_literal(old_literal(Literal)) :-
	unregister_literal(Literal).
monitor_literal(transaction(begin, reset)) :-
	rdf_monitor(monitor_literal, [-old_literal]),
	clean_token_index.
monitor_literal(transaction(end, reset)) :-
	rdf_monitor(monitor_literal, [+old_literal]).

%%	register_literal(+Literal)
%
%	Associate the tokens of a literal with the literal itself.

register_literal(Literal) :-
	(   rdf_tokenize_literal(Literal, Tokens)
	->  text_of(Literal, Lang, Text),
	    literal_map(token, Map),
	    add_tokens(Tokens, Lang, Text, Map)
	;   true
	).

add_tokens([], _, _, _).
add_tokens([H|T], Lang, Literal, Map) :-
	rdf_insert_literal_map(Map, H, Literal, Keys),
	(   var(Keys)
	->  true
	;   forall(new_token(H, Lang), true),
	    (	Keys mod 1000 =:= 0
	    ->	progress(Map, 'Tokens'),
		(   Keys mod 10000 =:= 0
		->  check_index_workers(rdf_literal_monitor_queue, Keys)
		;   true
		)
	    ;	true
	    )
	),
	add_tokens(T, Lang, Literal, Map).


%%	unregister_literal(+Literal)
%
%	Literal is removed from the database.   As we abstract from lang
%	and type qualifiers we first have to  check this is the last one
%	that is destroyed.

unregister_literal(Literal) :-
	text_of(Literal, _Lang, Text),
	(   rdf(_,_,literal(Text))
	->  true			% still something left
	;   rdf_tokenize_literal(Literal, Tokens),
	    literal_map(token, Map),
	    del_tokens(Tokens, Text, Map)
	).

del_tokens([], _, _).
del_tokens([H|T], Literal, Map) :-
	rdf_delete_literal_map(Map, H, Literal),
	del_tokens(T, Literal, Map).


%%	rdf_tokenize_literal(+Literal, -Tokens) is semidet.
%
%	Tokenize a literal. We make  this   hookable  as tokenization is
%	generally domain dependent.

rdf_tokenize_literal(Literal, Tokens) :-
	tokenization(Literal, Tokens), !.		% Hook
rdf_tokenize_literal(Literal, Tokens) :-
	text_of(Literal, _Lang, Text),
	atom(Text),
	tokenize_atom(Text, Tokens0),
	select_tokens(Tokens0, Tokens).

select_tokens([], []).
select_tokens([H|T0], T) :-
	(   exclude_from_index(token, H)
	->  select_tokens(T0, T)
	;   number(H)
	->  (   integer(H),
	        between(-1073741824, 1073741823, H)
	    ->	T = [H|T1],
		select_tokens(T0, T1)
	    ;   select_tokens(T0, T)
	    )
	;   atom_length(H, 1)
	->  select_tokens(T0, T)
	;   no_index_token(H)
	->  select_tokens(T0, T)
	;   T = [H|T1],
	    select_tokens(T0, T1)
	).


%%	no_index_token(?Token)
%
%	Tokens we do not wish to index,   as  they creat huge amounts of
%	data with little or no value.  Is   there  a more general way to
%	describe this? Experience shows that simply  word count is not a
%	good criterium as it often rules out popular domain terms.

no_index_token(and).
no_index_token(an).
no_index_token(or).
no_index_token(of).
no_index_token(on).
no_index_token(in).
no_index_token(this).
no_index_token(the).


%%	text_of(+LiteralArg, -Lang, -Text) is semidet.
%
%	Get the textual  or  (integer)   numerical  information  from  a
%	literal value. Lang  is  the  language   to  use  for  stemming.
%	Currently we use English for untyped  plain literals or literals
%	typed xsd:string. Formally, these should not be tokenized, but a
%	lot of data out there does not tag strings with their language.

text_of(type(xsd:string, Text), en, Text) :- !.
text_of(type(_, Text), -, Text) :- !.
text_of(lang(Lang, Text), Lang, Text) :- !.
text_of(Text, en, Text) :- atom(Text), !.
text_of(Text, -, Text) :- integer(Text).


		 /*******************************
		 *	   STEM INDEX		*
		 *******************************/

%%	stem_index(-Map) is det.
%
%	Get the stemming literal index. This index is created on demand.
%	If some thread is creating the index, other threads wait for its
%	completion.

stem_index(Map) :-
	literal_map(stem, Map), !,
	wait_for_map(stem).
stem_index(Map) :-
	rdf_new_literal_map(Map),
	assert(literal_map(stem, Map)),
	assert((new_token(Token, Lang) :- add_stem(Token, Lang, Map))),
	message_queue_create(Queue),
	assert(map_building(stem, Queue)),
	thread_create(fill_stem_index(Map, Queue), _,
		      [ alias('__rdf_stemmer'),
			detached(true)
		      ]),
	wait_for_map(stem).

wait_for_map(MapName) :-
	(   map_building(MapName, Queue)
	->  catch(thread_get_message(Queue, _), _, true),
	    wait_for_map(MapName)
	;   true
	).

fill_stem_index(StemMap, Queue) :-
	call_cleanup(
	    forall(rdf_current_literal(Literal),
		   stem_literal_tokens(Literal, StemMap)),
	    ( message_queue_destroy(Queue),
	      retractall(map_building(stem, _)))).

stem_literal_tokens(Literal, StemMap) :-
	rdf_tokenize_literal(Literal, Tokens),
	text_of(Literal, Lang, _Text),
	insert_tokens_stem(Tokens, Lang, StemMap).

insert_tokens_stem([], _, _).
insert_tokens_stem([Token|T], Lang, Map) :-
	(   atom(Token)
	->  (   stem(Token, Lang, Stem)
	    ->	rdf_insert_literal_map(Map, Stem, Token, Keys),
		(   integer(Keys),
		    Keys mod 1000 =:= 0
		->  progress(Map, 'Stem')
		;   true
		)
	    ;	true
	    )
	;   true
	),
	insert_tokens_stem(T, Lang, Map).


add_stem(Token, Lang, Map) :-
	stem(Lang, Token, Stem),
	rdf_insert_literal_map(Map, Stem, Token, _).

:- if(current_predicate(snowball/3)).
stem(Token, LangSpec, Stem) :-
	main_lang(LangSpec, Lang),
	catch(snowball(Lang, Token, Stem), _, fail).
:- else.
stem(Token, _Lang, Stem) :-
	porter_stem(Token, Stem).
:- endif.

main_lang(LangSpec, Lang) :-
	sub_atom(LangSpec, Before, _, _, -), !,
	sub_atom(LangSpec, 0, Before, _, Lang).
main_lang(LangSpec, Lang) :-
	downcase_atom(LangSpec, Lang).


		 /*******************************
		 *	  METAPHONE INDEX	*
		 *******************************/


metaphone_index(Map) :-
	literal_map(metaphone, Map), !,
	wait_for_map(metaphone).
metaphone_index(Map) :-
	rdf_new_literal_map(Map),
	assert(literal_map(metaphone, Map)),
	assert((new_token(Token, Lang) :- add_metaphone(Token, Lang, Map))),
	message_queue_create(Queue),
	assert(map_building(metaphone, Queue)),
	thread_create(fill_metaphone_index(Map, Queue), _,
		      [ alias('__rdf_metaphone_indexer'),
			detached(true)
		      ]),
	wait_for_map(metaphone).

fill_metaphone_index(MetaphoneMap, Queue) :-
	call_cleanup(
	    fill_metaphone_index(MetaphoneMap),
	    ( message_queue_destroy(Queue),
	      retractall(map_building(metaphone, _)))).

fill_metaphone_index(MetaphoneMap) :-
	token_index(TokenMap),
	rdf_keys_in_literal_map(TokenMap, all, Tokens),
	metaphone(Tokens, MetaphoneMap).

metaphone([], _).
metaphone([Token|T], Map) :-
	(   atom(Token),
	    double_metaphone(Token, SoundEx)
	->  rdf_insert_literal_map(Map, SoundEx, Token, Keys),
	    (	integer(Keys),
		Keys mod 1000 =:= 0
	    ->	progress(Map, 'Metaphone')
	    ;	true
	    )
	;   true
	),
	metaphone(T, Map).


add_metaphone(Token, _Lang, Map) :-
	double_metaphone(Token, SoundEx),
	rdf_insert_literal_map(Map, SoundEx, Token).


%%	rdf_literal_index(+Type, -Index) is det.
%
%	True when Index is a literal map   containing the index of Type.
%	Type is one of:
%
%	  - token
%	  Tokens are basically words of literal values. See
%	  rdf_tokenize_literal/2.  The `token` map maps tokens to full
%	  literal texts.
%	  - stem
%	  Index of stemmed tokens.  If the language is available, the
%	  tokens are stemmed using the matching _snowball_ stemmer.
%	  The `stem` map maps stemmed to full tokens.
%	  - metaphone
%	  Phonetic index of tokens.  The `metaphone` map maps phonetic
%	  keys to tokens.

rdf_literal_index(token, Map) :- !,
	token_index(Map).
rdf_literal_index(stem, Map) :- !,
	stem_index(Map).
rdf_literal_index(metaphone, Map) :- !,
	metaphone_index(Map).
rdf_literal_index(Type, _Map) :-
	domain_error(literal_index, Type).


		 /*******************************
		 *	       UTIL		*
		 *******************************/

verbose(Fmt, Args) :-
	setting(verbose(true)), !,
	format(user_error, Fmt, Args).
verbose(_, _).

progress(Map, Which) :-
	setting(verbose(true)), !,
	rdf_statistics_literal_map(Map, size(Keys, Values)),
	format(user_error,
	       '\r~t~w: ~12|Keys: ~t~D~15+; Values: ~t~D~20+',
	       [Which, Keys, Values]).
progress(_,_).


		 /*******************************
		 *	      SANDBOX		*
		 *******************************/

:- multifile sandbox:safe_primitive/1.

sandbox:safe_primitive(rdf_litindex:rdf_find_literals(_,_)).
sandbox:safe_primitive(rdf_litindex:rdf_tokenize_literal(_,_)).
sandbox:safe_primitive(rdf_litindex:rdf_literal_index(_,_)).
