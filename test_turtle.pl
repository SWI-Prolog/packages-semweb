/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2009-2015, University of Amsterdam
                              VU University Amsterdam
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

:- module(test_turtle,
          [ test_turtle/0,
            test_turtle/1               % +Test
          ]).
:- include(local_test).

fix_load_path :-
    prolog_load_context(directory, Dir),
    file_base_name(Dir, LocalDir),
    LocalDir \== semweb,
    !,
    asserta(system:term_expansion((:- use_module(library(semweb/X))),
                                  (:- use_module(library(LocalDir/X))))).
fix_load_path.

:- fix_load_path.

:- use_module(turtle).
:- use_module(rdf_db).
:- use_module(rdf_compare).
:- use_module(library(rdf_ntriples)).
:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(aggregate)).

:- dynamic
    error/1,
    passed/1,
    this_dir/1.

:- retractall(this_dir(_)),
   prolog_load_context(directory, Dir),
   asserta(this_dir(Dir)).


test_turtle :-
    retractall(error(_)),
    retractall(passed(_)),
    this_dir(Dir),
    atom_concat(Dir, '/Tests/Turtle', TestDir),
    test_dir(TestDir),
    garbage_collect_atoms,                  % leak checks
    (   error(_)
    ->  fail
    ;   aggregate_all(count, passed(_), Passed),
        aggregate_all(count, blocked(_), Blocked),
        format('~NAll ~D Turtle tests passed (~D blocked)~n',
               [Passed, Blocked])
    ).

test_turtle(File) :-
    this_dir(Here),
    atomic_list_concat([Here, '/Tests/Turtle/', File], FullFile),
    test_file(FullFile).

%!  blocked(?Test)
%
%   True if Test is blocked.

:- dynamic
    blocked/1.

%blocked('test-28.ttl').


%:- debug(test_turtle).

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Handle the test-cases provided with the Turtle language definition.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

test_dir(Dir) :-
    atom_concat(Dir, '/*.ttl', Pattern),
    expand_file_name(Pattern, Files),
    maplist(test_file, Files).

test_file(File) :-
    file_base_name(File, BaseName),
    blocked(BaseName),
    !,
    print_message(informational, test_turtle(blocked, BaseName)).
test_file(File) :-
    file_base_name(File, Base),
    atom_concat(bad, _, Base),
    !,
    file_base_name(File, BaseName),
    debug(test_turtle, 'Negative test ~w ...', [BaseName]),
    catch(load_turtle(File, _Triples), E, true),
    (   nonvar(E)
    ->  test_passed(BaseName)
    ;   print_message(error, test_turtle(false, BaseName))
    ).
test_file(File) :-
    file_base_name(File, BaseName),
    file_name_extension(Base, ttl, File),
    file_name_extension(Base, out, OkFile),
    exists_file(OkFile),
    !,
    debug(test_turtle, 'Test ~w ...', [BaseName]),
    load_turtle(File, Triples),
    load_rdf_ntriples(OkFile, OkTriples0),
    maplist(canonical_triple, OkTriples0, OkTriples),
    sort(Triples, Turtle),
    sort(OkTriples, OK),
    (   rdf_equal_graphs(OK, Turtle, _)
    ->  test_passed(BaseName)
    ;   print_message(error, test_turtle(false, BaseName)),
        (   debugging(test_turtle)
        ->  report_diff(OK, Turtle)
        ;   true
        )
    ).
test_file(_).                           % not a test

load_turtle(File, Triples) :-
    file_base_name(File, Base),
    atom_concat('http://www.w3.org/2001/sw/DataAccess/df1/tests/',
                Base,
                BaseURI),
    rdf_read_turtle(File, Triples,
                    [ base_uri(BaseURI),
                      anon_prefix(node(_)),
                      on_error(error)
                    ]).

canonical_triple(rdf(S0, P0, O0),
                 rdf(S,  P,  O)) :-
    canonical_node(S0, S),
    canonical_node(P0, P),
    canonical_node(O0, O).

canonical_node(node(GenId), node(N)) :-
    atom_concat(genid, AN, GenId),
    !,
    atom_number(AN, N).
canonical_node(Node, Node).

report_diff(OK, Result) :-
    rdf_equal_graphs(OK, Result, _),
    !.
report_diff(OK, Result) :-
    subtract(OK, Result, Missing),
    subtract(Result, OK, TooMany),
    (   Missing \== []
    ->  length(Missing, NM),
        format('**************** ~D Omitted results:~n', [NM]),
        write_list(Missing)
    ;   true
    ),
    (   TooMany \== []
    ->  length(TooMany, TM),
        format('**************** ~D Overcomplete results:~n', [TM]),
        write_list(TooMany)
    ;   true
    ).

write_list([]).
write_list([H|T]) :-
    (   H =.. [row|Cols]
    ->  write_cols(Cols),
        format(' .~n')
    ;   H = rdf(S,P,O)
    ->  write_cell(S), put(' '),
        write_cell(P), put(' '),
        write_cell(O), write(' .\n')
    ;   format('~p~n', [H])
    ),
    write_list(T).


write_cols([]).
write_cols([H|T]) :-
    write_cell(H),
    (   T == []
    ->  true
    ;   put(' '),
        write_cols(T)
    ).

write_cell(literal(X)) :-
    !,
    format('"~w"', [X]).
write_cell(R) :-
    atom(R),
    rdf_global_id(NS:Id, R),
    !,
    format('<~w:~w>', [NS, Id]).
write_cell('$null$') :-
    !,
    write('NULL').
write_cell(R) :-
    atom(R),
    !,
    format('<!~w>', [R]).
write_cell(X) :-
    format('~p', [X]).


                 /*******************************
                 *            MESSAGES          *
                 *******************************/

test_passed(Test) :-
    print_message(informational, test_turtle(true, Test)),
    (   current_prolog_flag(verbose, silent)
    ->  put_char(user_error, '.')
    ;   true
    ).

:- multifile
    prolog:message//1.

prolog:message(test_turtle(true, Test)) -->
    { assert(passed(Test)) },
    [ 'Turtle test ~q: ~tpassed~42|'-[Test] ].
prolog:message(test_turtle(false, Test)) -->
    { assert(error(Test)) },
    [ 'Turtle test ~q: ~tFAILED~42|'-[Test], nl,
      'Re-run with "?- debug(test_turtle)." to see more details'-[]
    ].
prolog:message(test_turtle(blocked, Test)) -->
    [ 'Turtle test ~q: ~t(blocked)~42|'-[Test] ].
