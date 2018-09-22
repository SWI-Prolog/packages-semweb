/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2006-2018, VU University Amsterdam
			      CWI, Amsterdam
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(test,
          [ test_litmap/0,
            test_litmap/1,
            test_litmap/2
          ]).
:- include(local_test).
:- use_module(library(debug)).
:- use_module(library(semweb/rdf_db)).

:- dynamic
    map/1,                          % the literal map
    map/2,                          % Prolog shadow admin
    in/1,                           % Read generated random
    out/1.                          % Write generated random

test_litmap :-
    test_litmap(10000),
    format('~NLiteral map tests passed~n').

test_litmap(N, log(File)) :-
    open(File, write, Out),
    assert(out(Out)),
    call_cleanup(test(N),
                 (   close(Out),
                     retractall(out(_)))).
test_litmap(N, from(File)) :-
    open(File, read, In),
    assert(in(In)),
    call_cleanup(test(N),
                 (   close(In),
                     retractall(in(_)))).

test_litmap(N) :-
    mk(N),
    (   v(continue),
        vk(all),
        vk(prefix('1')),
        vk(ge(500)),
        vk(le(500)),
        vk(between(100, 500))
    ->  clear
    ).

clear :-
    (   retract(map(Map))
    ->  rdf_destroy_literal_map(Map)
    ;   true
    ),
    retractall(map(_,_)).

mk(N) :-
    clear,
    rdf_new_literal_map(Map),
    assert(map(Map)),
    forall(between(1, N, _), m1(N, Map)).

m1(N, Map) :-
    KeyRange is N//10,
    ValRange is N//100,
    rnd_value(KeyRange, Key),
    rnd_value(ValRange, Value),
    (   random(2, 0)
    ->  (   retract(map(Key, Value))
        ->  debug(delete, 'Deleted ~q --> ~q', [Key, Value]),
            rdf_delete_literal_map(Map, Key, Value)
        ;   true
        )
    ;   (   map(Key, Value)
        ->  true
        ;   assert(map(Key, Value))
        ),
        rdf_insert_literal_map(Map, Key, Value)
    ).

rnd_value(Max, Value) :-
    random(Max, ValueI),
    (   random(2, 0)
    ->  atom_number(Value, ValueI)
    ;   Value = ValueI
    ).

v(Stop) :-
    setof(X, Y^map(X, Y), Xs),
    forall(member(X, Xs),
           v(X, Stop)).

v(Key, Stop) :-
    map(Map),
    findall(V, map(Key, V), Vs),
    sort(Vs, VsS),
    rdf_find_literal_map(Map, [Key], Vs2),
    sort(Vs2, Vs2S),
    (   Vs2S == VsS
    ->  true
    ;   format('~q: ~q (must be ~q)~n', [Key, Vs2S, VsS]),
        Stop == continue
    ).

vk(all) :-
    map(Map),
    rdf_keys_in_literal_map(Map, all, Keys),
    setof(X, Y^map(X, Y), Xs),
    (   Xs == Keys
    ->  true
    ;   (   ord_subtract(Xs, Keys, Missing),
            Missing \== []
        ->  format('Missing: ~p~n', [Missing])
        ;   true
        ),
        (   ord_subtract(Keys, Xs, TooMany),
            TooMany \== []
        ->  format('TooMany: ~p~n', [TooMany])
        ;   true
        ),
        fail
    ).
vk(prefix(Prefix)) :-
    map(Map),
    rdf_keys_in_literal_map(Map, prefix(Prefix), Keys),
    prefix_keys(Prefix, KeysOK),
    (   KeysOK == Keys
    ->  true
    ;   format('prefix(~w): ~p (must be ~p)~n', [Prefix, Keys, KeysOK]),
        fail
    ).
vk(ge(Min)) :-
    map(Map),
    rdf_keys_in_literal_map(Map, ge(Min), Keys),
    between_keys(Min, 0x5fffffff, KeysOK),
    (   KeysOK == Keys
    ->  true
    ;   format('ge(~w): ~p (must be ~p)~n', [Min, Keys, KeysOK]),
        fail
    ).
vk(le(Max)) :-
    map(Map),
    rdf_keys_in_literal_map(Map, le(Max), Keys),
    between_keys(-0x60000000, Max, KeysOK),
    (   KeysOK == Keys
    ->  true
    ;   format('le(~w): ~p (must be ~p)~n', [Max, Keys, KeysOK]),
        fail
    ).
vk(between(Min, Max)) :-
    map(Map),
    rdf_keys_in_literal_map(Map, between(Min, Max), Keys),
    between_keys(Min, Max, KeysOK),
    (   KeysOK == Keys
    ->  true
    ;   format('between(~w, ~w): ~p (must be ~p)~n',
               [Min, Max, Keys, KeysOK]),
        fail
    ).

prefix_keys(Prefix, Keys) :-
    findall(K, prefix_key(Prefix, K), Keys0),
    sort(Keys0, Keys).

prefix_key(Prefix, Key) :-
    map(Key,_),
    atom(Key),
    sub_atom(Key, 0, _, _, Prefix).

between_keys(Min, Max, Keys) :-
    findall(K, between_key(Min, Max, K), Keys0),
    sort(Keys0, Keys).

between_key(Min, Max, Key) :-
    map(Key,_),
    integer(Key),
    between(Min, Max, Key).


random(Max, Value) :-
    in(Stream),
    !,
    read(Stream, Term),
    assertion(Term \== end_of_file),
    Term = xx(Max0, Value),
    assertion(Max == Max0).
random(Max, Value) :-
    out(Stream),
    !,
    Value0 is random(Max),
    format(Stream, 'xx(~q, ~q).~n', [Max, Value0]),
    Value = Value0.
random(Max, Value) :-
    Value is random(Max).

