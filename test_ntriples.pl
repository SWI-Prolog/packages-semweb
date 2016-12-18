/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2013, VU University Amsterdam
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

:- module(test_ntriples,
          [ atom_triple/2,              % +Atom, -Triple
            test_ntriples/0
          ]).
:- include(local_test).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(plunit)).
:- use_module(library(memfile)).

test_ntriples :-
    run_tests([ positive,
                negative
              ]).

atom_triple(Atom, Triple) :-
    setup_call_cleanup(
        atom_to_memory_file(Atom, MF),
        setup_call_cleanup(
            open_memory_file(MF, read, In),
            read_ntriple(In, Triple),
            close(In)),
        free_memory_file(MF)).

:- begin_tests(positive).

test(basic, T == triple(a,b,c)) :-
    atom_triple('<a> <b> <c> .', T).
test(comment, T == triple(a,b,c)) :-
    atom_triple('#hello\n<a> <b> <c> .', T).
test(comment, T == triple(a,b,c)) :-
    atom_triple('#hello\r\n<a> <b> <c> .', T).
test(comment, T == triple(a,b,c)) :-
    atom_triple('#hello\r<a> <b> <c> .', T).
test(blank_line, T == triple(a,b,c)) :-
    atom_triple('\n<a> <b> <c> .', T).
test(blank_line, T == triple(a,b,c)) :-
    atom_triple('\n  \t<a> <b> <c> .', T).
test(node, T == triple(node(a),b,c)) :-
    atom_triple('_:a <b> <c> .', T).
test(plain, T == triple(a,b,literal(hello))) :-
    atom_triple('<a> <b> "hello" .', T).
test(lang, T == triple(a,b,literal(lang(en, hello)))) :-
    atom_triple('<a> <b> "hello"@en .', T).
test(type, T == triple(a,b,literal(type(t, hello)))) :-
    atom_triple('<a> <b> "hello"^^<t> .', T).
test(empty, T == triple(a,b,literal(''))) :-
    atom_triple('<a> <b> "" .', T).
test(quote, T == triple(a,b,literal('"'))) :-
    atom_triple('<a> <b> "\\"" .', T).
test(quote, T == triple(a,b,literal('\n'))) :-
    atom_triple('<a> <b> "\\n" .', T).
test(quote, T == triple(a,b,literal('\r'))) :-
    atom_triple('<a> <b> "\\r" .', T).
test(quote, T == triple(a,b,literal('\\'))) :-
    atom_triple('<a> <b> "\\\\" .', T).
test(escape, T == triple(a,b,literal('\u1234'))) :-
    atom_triple('<a> <b> "\\u1234" .', T).
test(escape, T == triple(a,b,literal('\uABCD'))) :-
    atom_triple('<a> <b> "\\uABCD" .', T).
test(escape, T == triple(a,b,literal('\U00012345'))) :-
    atom_triple('<a> <b> "\\U00012345" .', T).
test(eof, T == end_of_file) :-
    atom_triple('', T).
test(eof, T == end_of_file) :-
    atom_triple('  ', T).

:- end_tests(positive).

:- begin_tests(negative).

test(newline, error(syntax_error('newline in uriref'))) :-
    atom_triple('<a\n> <b> <c> .', _).
test(eof, error(syntax_error('EOF in string'))) :-
    atom_triple('<a> <b> "', _).
test(newline, error(syntax_error('newline in string'))) :-
    atom_triple('<a> <b> "\n', _).
test(nospace, error(syntax_error('subject not followed by whitespace'))) :-
    atom_triple('<a><b> <c> .', _).
test(nospace, error(syntax_error('predicate not followed by whitespace'))) :-
    atom_triple('<a> <b><c> .', _).
test(escape, error(syntax_error('illegal unicode escape'))) :-
    atom_triple('<a> <b> "\\u123" .', _).
test(escape, error(syntax_error('illegal escape'))) :-
    atom_triple('<a> <b> "\\x" .', _).
test(lang, error(syntax_error('language tag must start with a-zA-Z'))) :-
    atom_triple('<a> <b> "hello"@1 .', _).
test(lang, error(syntax_error('fullstop (.) expected'))) :-
    atom_triple('<a> <b> "hello"@e$ .', _).

:- end_tests(negative).
