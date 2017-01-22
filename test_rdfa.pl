/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2014-2015, VU University Amsterdam
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

:- module(test_rdfa,
          [ load_manifest/1,
            test/1,
            test/0
          ]).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdf_persistency)).
:- use_module(library(semweb/turtle)).
:- use_module(library(sgml)).
:- use_module(library(uri)).
:- use_module(library(apply)).
:- use_module(library(aggregate)).
:- use_module(library(debug)).
:- use_module(library(readutil)).
:- use_module(library(http/url_cache)).
:- use_module(rdfql(sparql)).

:- rdf_persistency(manifest, false).
:- rdf_persistency(data,     false).

:- rdf_register_prefix(
       mf,   'http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#').
:- rdf_register_prefix(
       qt,   'http://www.w3.org/2001/sw/DataAccess/tests/test-query#').
:- rdf_register_prefix(
       test, 'http://www.w3.org/2006/03/test-description#').

:- dynamic
    test_result/2,
    current_manifest/1.

/** <module> RDFa test driver

This module drives  the  tests   for  library(semweb/rdfa).  These tests
require ClioPatria because they need  SPARQL   processing.  To run these
tests, proceed as follows:

  1. Install ClioPatria
  2. Configure a default project, typically using

     ==
     ../ClioPatria/configure --with-localhost
     ==
  3. Add this file to the project directory
  4. Start ClioPatria using =|./run.pl|= and load this file using
     =|?- [test_rdfa].|=
  5. Load one of the manifests using e.g., the command below.
     Defined manifests are =html4=, =html5=, =xhtml5=, =svg=,
     =xhtml1= and =xml=.

     ==
     ?- load_manifest(xhtml5)
     ==

  6. Run all tests from the loaded manifest using =|?- test.|=

  7. To dig down into a specific test, run test(Number), where
     number is the integer of the test fragment id.  E.g.,
     =|?- test(42).|=  This print the test comment, input,
     extracted RDF (as Turtle), SPARQL query and final result.
*/

manifest(html4,  'http://rdfa.info/test-suite/rdfa1.1/html4/manifest').
manifest(html5,  'http://rdfa.info/test-suite/rdfa1.1/html5/manifest').
manifest(xhtml5, 'http://rdfa.info/test-suite/rdfa1.1/xhtml5/manifest').
manifest(svg,    'http://rdfa.info/test-suite/rdfa1.1/svg/manifest').
manifest(xhtml1, 'http://rdfa.info/test-suite/rdfa1.1/xhtml1/manifest').
manifest(xml,    'http://rdfa.info/test-suite/rdfa1.1/xml/manifest').

manifest_file_type(html4,  html).
manifest_file_type(html5,  html).
manifest_file_type(xhtml5, xml).
manifest_file_type(svg,    xml).
manifest_file_type(xhtml1, xml).
manifest_file_type(xml,    xml).

%!  blocked(?Test, ?Why)
%
%   Tests that we process  correctly,  but   for  which  the test is
%   inadequate.
%
%     * #0198 fails because our XMLLiteral does not include all
%       prefixes as xmlns declarations.
%     * #0311 fails because no triple is created, but there are
%       triples in other graphs (=manifest=)
%     * #0319 fails because we generate relative URLs, but the test
%       assumes they are turned absolute in a way that requires
%       Turtle serialization and parsing.

term_expansion(blocked(Test, Why), blocked(URI, Why)) :-
    atom_concat('http://rdfa.info/test-suite/test-cases/rdfa1.1/',
                Test, URI).

blocked('xhtml5/manifest#0198', xmlliteral).
blocked('xhtml5/manifest#0311', eval).
blocked('xhtml5/manifest#0319', eval).
blocked('xhtml1/manifest#0198', xmlliteral).
blocked('xhtml1/manifest#0311', eval).
blocked('xhtml1/manifest#0319', eval).
blocked('xml/manifest#0311',    eval).
blocked('xml/manifest#0319',    eval).
blocked('svg/manifest#0311',    eval).
blocked('html5/manifest#0099',  white_space_preserve).
blocked('html5/manifest#0112',  white_space_preserve).
blocked('html5/manifest#0253',  white_space_preserve).
blocked('html5/manifest#0311',  eval).
blocked('html5/manifest#0329',  white_space_preserve).

load_manifest(Type) :-
    retractall(current_manifest(_)),
    rdf_unload_graph(manifest),
    manifest(Type, Manifest),
    url_cache(Manifest, ManifestFile, _Type),
    rdf_load(ManifestFile, [format(turtle), graph(manifest)]),
    asserta(current_manifest(Type)).

test :-
    retractall(test_result(_,_)),
    forall(test(_), true),
    aggregate_all(count, test_result(_, _), Total),
    aggregate_all(count, test_result(_, passed), Passed),
    aggregate_all(count, test_result(_, failed(_,_)), Failed),
    Eval is Total - Passed - Failed,
    Success is (Passed*100)/Total,
    format('Processed ~D tests, ~D failed ~D eval issues (~2f% success)~n',
           [Total, Failed, Eval, Success]).

test(Num) :-
    (   nonvar(Num)
    ->  Cut = true,
        (   debugging(rdfa(test))
        ->  Cleanup = true
        ;   debug(rdfa(test)),
            Cleanup = nodebug(rdfa(test))
        )
    ;   Cleanup = true
    ),
    freeze(Fragment, atom_number(Fragment, Num)),
    rdf(Test, rdf:type, mf:'QueryEvaluationTest'),
    uri_components(Test, Components),
    uri_data(fragment, Components, Fragment),
    (   Cut == true
    ->  !
    ;   true
    ),
    call_cleanup(run_test(Test), Cleanup).

run_test(Test) :-
    rdf(Test, mf:action, Action),
    rdf(Action, qt:data, Data),
    url_cache(Data, DataFile, _Type),
    (   debugging(rdfa(test))
    ->  (   rdf(Test, rdfs:comment, literal(Comment))
        ->  format('~w~n~n', [Comment])
        ;   true
        ),
        cat(DataFile)
    ;   true
    ),
    (   current_manifest(Manifest),
        manifest_file_type(Manifest, xml)
    ->  load_xml(DataFile, DOM, [])
    ;   load_html(DataFile, DOM, [])
    ),
    (   xml_rdfa(DOM, Triples, [base(Data)])
    ->  (   debugging(rdfa(test))
        ->  format('~N~n'),
            write_turle(Triples, [base(Data)])
        ;   true
        ),
        validate_test(Test, Triples)
    ;   rdf(Test, mf:result, literal(type(xsd:boolean, Thruth))),
        test_failed(Test, Thruth, parse_failed)
    ).


validate_test(Test, Triples) :-
    rdf(Test, mf:action, Action),
    rdf(Action, qt:query, Query),
    url_cache(Query, QueryFile, _Type),
    assert_triples(Triples),
    (   debugging(rdfa(test))
    ->  cat(QueryFile)
    ;   true
    ),
    read_file_to_codes(QueryFile, QueryText, []),
    sparql_query(QueryText, Reply, [entailment(none)]),
    rdf(Test, mf:result, literal(type(xsd:boolean, Thruth))),
    (   Reply == Thruth
    ->  test_ok(Test, passed)
    ;   blocked(Test, Why)
    ->  test_ok(Test, Why)
    ;   test_failed(Test, Thruth, Reply)
    ).


assert_triples(Triples) :-
    rdf_retractall(_,_,_,data),
    maplist(assert_in(data), Triples).

assert_in(Graph, rdf(S,P,O)) :-
    rdf_assert(S,P,O,Graph).



cat(File) :-
    open(File, read, In),
    copy_stream_data(In, current_output),
    close(In).

test_failed(Test, Expected, Result) :-
    assert(test_result(Test, failed(Expected,Result))),
    print_message(error, format('Test failed: ~p (~p \\== ~p)',
                                [ Test, Expected, Result ])).

test_ok(Test, Why) :-
    assertz(test_result(Test, Why)).


%!  write_turle(+Triples:list, +Options) is det.
%
%   Write triples in RDF format

write_turle(Triples, Options) :-
    rdf_save_turtle(stream(current_output),
                    [ expand(triple_in(Triples))
                    | Options
                    ]).

triple_in(RDF, S,P,O,_G) :-
    member(rdf(S,P,O), RDF).
