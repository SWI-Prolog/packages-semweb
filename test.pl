/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        wielemak@science.uva.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 1985-2012, University of Amsterdam
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
