/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2018, VU University Amsterdam
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

:- module(subprop,
          [ subprop/0
          ]).
:- use_module(library(semweb/rdf_db)).

rdf_db:ns(test, 'http://www.test.org/').

% :- rdf_debug(10).

t1 :-
    rdf_assert(test:a, rdfs:subPropertyOf, test:r1),
    rdf_assert(test:jan, test:a, literal(jan)).

t2 :-
    rdf_assert(test:a, rdfs:subPropertyOf, test:r1),
    rdf_assert(test:a, rdfs:subPropertyOf, test:r2),
    rdf_assert(test:jan, test:a, literal(jan)).

t3 :-
    rdf_assert(test:a, rdfs:subPropertyOf, test:r1),
    rdf_assert(test:a, rdfs:subPropertyOf, test:r2),
    rdf_assert(test:b, rdfs:subPropertyOf, test:r3),
    rdf_assert(test:b, rdfs:subPropertyOf, test:r4),
    rdf_assert(test:c, rdfs:subPropertyOf, test:a),
    rdf_assert(test:c, rdfs:subPropertyOf, test:b),
    rdf_assert(test:jan, test:a, literal(jan)).

subprop :-
    rdf_reset_db,
    t3,
    rdf_has(test:jan, test:r1, Name),
    Name == literal(jan).
