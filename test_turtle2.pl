/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2013-2017, VU University Amsterdam
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

:- module(test_turtle2,
          [ test_turtle2/0,
            edit_test/1,                % +Name
            edit_result/1,              % +Name
            parse_test/2,               % +Test, -Triples
            parse_result/2              % +Test, -Triples
          ]).
:- include(local_test).
:- use_module(library(uri)).
:- use_module(library(debug)).
:- use_module(library(apply)).
:- use_module(library(aggregate)).
:- use_module(library(semweb/turtle)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdf_portray), []).
:- use_module(library(semweb/rdf_compare)).
:- use_module(library(semweb/rdf_ntriples)).

:- debug(turtle).
:- thread_local passed/1, error/1, blocked/1.

:- rdf_register_prefix(
       mf,
       'http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#').
:- rdf_register_prefix(
       rdft,
       'http://www.w3.org/ns/rdftest#').

/** <module>  RDF-WG test-suite for Turtle

Run the RDF-WG test-suite for Turtle. The   test is executed only if the
environment variable =TEST_TURTLE_MANIFEST= points to   an existince W3C
manifest file.  The RDF specs and test-cases can be downloaded using

        hg clone https://dvcs.w3.org/hg/rdf

The manifest is in =rdf-turtle/tests-ttl/manifest.ttl= in the downloaded
repository.
*/

test_turtle2 :-
    getenv('TEST_TURTLE_MANIFEST', Manifest),
    exists_file(Manifest),
    !,
    rdf_reset_db,
    retractall(passed(_)),
    retractall(error(_)),
    retractall(blocked(_)),
    rdf_load(Manifest),
    syntax_tests,
    negative_syntax_tests,
    eval_tests,
    negative_eval_tests,
    garbage_collect_atoms,                  % leak checks
    (   error(_)
    ->  fail
    ;   aggregate_all(count, passed(_), Passed),
        aggregate_all(count, blocked(_), Blocked),
        format('~NAll ~D Turtle tests passed (~D blocked)~n',
               [Passed, Blocked])
    ).
test_turtle2 :-
    format('No W3C test files present; skipping Turtle tests~n').

blocked_test('turtle-eval-bad-01').
blocked_test('turtle-eval-bad-02').
blocked_test('turtle-eval-bad-03').

%!  syntax_tests
%
%   Run the positive syntax tests.

syntax_tests :-
    forall(rdf(T, rdf:type, rdft:'TestTurtlePositiveSyntax'),
           syntax_test(T)).

syntax_test(T) :-
    rdf(T, mf:action, FileURI),
    rdf(T, mf:name, literal(Name)),
    uri_file_name(FileURI, File),
    catch(rdf_read_turtle(File, _Triples,
                          [ on_error(error)
                          ]),
          E,
          (   print_message(error, E),
              print_message(error, test_turtle(false, Name))
          )),
    (   var(E)
    ->  test_passed(Name)
    ;   true
    ).

%!  negative_syntax_tests
%
%   Run the negative syntax tests.

negative_syntax_tests :-
    forall(rdf(T, rdf:type, rdft:'TestTurtleNegativeSyntax'),
           negative_syntax_test(T)).

negative_syntax_test(T) :-
    rdf(T, mf:action, FileURI),
    rdf(T, mf:name, literal(Name)),
    uri_file_name(FileURI, File),
    catch(rdf_read_turtle(File, _Triples,
                          [ on_error(error)
                          ]),
          E, true),
    (   var(E)
    ->  print_message(error, test_turtle(false, Name))
    ;   test_passed(Name)
    ).

%!  negative_eval_tests
%
%   Run the negative evaluation tests.

negative_eval_tests :-
    forall(rdf(T, rdf:type, rdft:'TestTurtleNegativeEval'),
           negative_eval_test(T)).

negative_eval_test(T) :-
    rdf(T, mf:name, literal(Name)),
    blocked_test(Name),
    print_message(informational, test_turtle(blocked, Name)).
negative_eval_test(T) :-
    rdf(T, mf:action, FileURI),
    rdf(T, mf:name, literal(Name)),
    uri_file_name(FileURI, File),
    catch(rdf_read_turtle(File, _Triples,
                          [ on_error(error)
                          ]),
          E, true),
    (   var(E)
    ->  print_message(error, test_turtle(false, Name))
    ;   test_passed(Name)
    ).

%!  eval_tests
%
%   Run the evaluation tests.

eval_tests :-
    forall(rdf(T, rdf:type, rdft:'TestTurtleEval'),
           eval_test(T)).

eval_test(T) :-
    rdf(T, mf:action, ActionURI),
    rdf(T, mf:result, ResultURI),
    rdf(T, mf:name, literal(Name)),
    !,
    uri_file_name(ActionURI, Input),
    uri_file_name(ResultURI, Output),
    file_base_name(Input, Base),
    atom_concat('http://example/base/', Base, BaseURI),
    catch(rdf_read_turtle(Input, Triples,
                          [ on_error(error),
                            base_uri(BaseURI),
                            anon_prefix(node(_))
                          ]),
          E,
          (   print_message(error, E),
              print_message(error, test_turtle(false, Name)),
              fail
          )),
    rdf_read_ntriples(Output, OkTriples0, [anon_prefix(node(_))]),
    maplist(canonical_triple, OkTriples0, OkTriples),
    sort(Triples, Turtle),
    sort(OkTriples, OK),
    (   rdf_equal_graphs(OK, Turtle, _)
    ->  test_passed(Name)
    ;   print_message(error, test_turtle(false, Name)),
        (   debugging(test_turtle)
        ->  report_diff(OK, Turtle)
        ;   true
        )
    ).
eval_test(Name) :-
    rdf(T, mf:name, literal(Name)),
    !,
    eval_test(T).
eval_test(T) :-
    existence_error(test, T).


                 /*******************************
                 *      RESULT COMPARISON       *
                 *******************************/

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
    format('~w:~w', [NS, Id]).
write_cell('$null$') :-
    !,
    write('NULL').
write_cell(R) :-
    atom(R),
    !,
    format('<~w>', [R]).
write_cell(X) :-
    format('~p', [X]).


                 /*******************************
                 *          DEBUG  HELP         *
                 *******************************/

%!  edit_test(+Name)
%
%   Edit the input file for a test.

edit_test(Name) :-
    rdf(T, mf:name, literal(Name)),
    rdf(T, mf:action, FileURI),
    uri_file_name(FileURI, File),
    edit(File).

%!  edit_result(+Name)
%
%   Edit the output file for a test.

edit_result(Name) :-
    rdf(T, mf:name, literal(Name)),
    rdf(T, mf:result, FileURI),
    uri_file_name(FileURI, File),
    edit(File).

%!  parse_test(+Name, -Triples)
%
%   Parse the input for the test named   Name and unify Triples with
%   the result.

parse_test(Name, Triples) :-
    rdf(T, mf:name, literal(Name)),
    rdf(T, mf:action, FileURI),
    uri_file_name(FileURI, File),
    rdf_read_turtle(File, Triples, []).

%!  parse_result(+Name, -Triples)
%
%   Parse the results for the test named Name and unify Triples with
%   the result.

parse_result(Name, Triples) :-
    rdf(T, mf:name, literal(Name)),
    rdf(T, mf:result, FileURI),
    uri_file_name(FileURI, File),
    rdf_read_ntriples(File, Triples, [anon_prefix(node(_))]).


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

prolog:message(turtle(neg_succeeded(Name))) -->
    [ 'Negative syntax test ~q succeeded'-[Name] ].
prolog:message(test_turtle(true, Test)) -->
    { assert(passed(Test)) },
    [ 'Turtle test ~q: ~`.tpassed~72|'-[Test] ].
prolog:message(test_turtle(false, Test)) -->
    { assert(error(Test)) },
    [ 'Turtle test ~q: ~`.tFAILED~72|'-[Test] ],
    show_debugging.
prolog:message(test_turtle(blocked, Test)) -->
    { assert(blocked(Test)) },
    [ 'Turtle test ~q: ~`.t(blocked)~72|'-[Test] ].

show_debugging -->
    { debugging(test_turtle) },
    !.
show_debugging -->
    [ nl,
      'Re-run with "?- debug(test_turtle)." to see more details'-[]
    ].
