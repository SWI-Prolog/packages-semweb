/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2007-2015, University of Amsterdam
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

:- include(local_test).

run_zlib_tests :-
    absolute_file_name(foreign(zlib4pl), _,
                       [ file_type(executable),
                         access(execute),
                         file_errors(fail)
                       ]).

run_network_tests :-
    \+ getenv('USE_PUBLIC_NETWORK_TESTS', false).

:- use_module(library(plunit)).
:- use_module(library(uri)).
:- use_module(library(semweb/rdf_db)).
:- if(run_zlib_tests).
:- use_module(rdf_zlib_plugin).
:- endif.
:- use_module(rdf_http_plugin).


:- begin_tests(load,
               [ setup(rdf_reset_db),
                 cleanup(rdf_reset_db)
               ]).

test(file, [true(N == 1), cleanup(rdf_reset_db)]) :-
    rdf_load('Tests/test-001.rdf', [silent(true)]),
    rdf_statistics(triples(N)).

test(file, [true(N == 1), cleanup(rdf_reset_db)]) :-
    uri_file_name(URI, 'Tests/test-001.rdf'),
    rdf_load(URI, [silent(true)]),
    rdf_statistics(triples(N)).

test(gzip_file, [condition(run_zlib_tests), true(N == 1), cleanup(rdf_reset_db)]) :-
    rdf_load('Tests/test-002.rdf', [silent(true)]),
    rdf_statistics(triples(N)).

test(gzip_file, [condition(run_zlib_tests), true(N == 1), cleanup(rdf_reset_db)]) :-
    uri_file_name(URI, 'Tests/test-002.rdf'),
    rdf_load(URI, [silent(true)]),
    rdf_statistics(triples(N)).

test(http, [condition(run_network_tests), true(N == 1), cleanup(rdf_reset_db)]) :-
    rdf_load('http://www.swi-prolog.org/Tests/semweb/test-001.rdf', [silent(true)]),
    rdf_statistics(triples(N)).

test(gzip_http, [condition((run_network_tests, run_zlib_tests)), true(N == 1), cleanup(rdf_reset_db)]) :-
    rdf_load('http://www.swi-prolog.org/Tests/semweb/test-002.rdf.gz', [silent(true)]),
    rdf_statistics(triples(N)).

:- end_tests(load).

:- begin_tests(inverse).

test(set,  [cleanup(rdf_reset_db)]) :-
    rdf_assert(r1, p1, r2),
    rdf_set_predicate(p2, inverse_of(p1)),
    rdf_has(r2, p2, r1).
test(clear,  [cleanup(rdf_reset_db)]) :-
    rdf_assert(r1, p1, r2),
    rdf_set_predicate(p2, inverse_of(p1)),
    rdf_has(r2, p2, r1),
    rdf_set_predicate(p2, inverse_of([])),
    \+ rdf_has(r2, p2, r1).

:- end_tests(inverse).
