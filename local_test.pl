/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2012-2016, VU University Amsterdam
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

assert_search_paths :-
    current_prolog_flag(xref, true),
    !.
assert_search_paths :-
    asserta(user:file_search_path(foreign, '../sgml')),
    asserta(user:file_search_path(foreign, '../clib')),
    asserta(user:file_search_path(foreign, '../zlib')),
    asserta(user:file_search_path(foreign, '../http')),
    asserta(user:file_search_path(library, '../sgml')),
    asserta(user:file_search_path(library, '../clib')),
    asserta(user:file_search_path(library, '../zlib')),
    asserta(user:file_search_path(library, '../RDF')),
    asserta(user:file_search_path(library, '../plunit')),
    asserta(user:file_search_path(library, '..')),
    asserta(user:file_search_path(foreign, '.')).

fix_load_path :-
    current_prolog_flag(xref, true),
    !.
fix_load_path :-
    prolog_load_context(directory, Dir),
    file_base_name(Dir, LocalDir),
    LocalDir \== semweb,
    !,
    asserta(system:term_expansion((:- use_module(library(semweb/X))),
                                  (:- use_module(library(LocalDir/X))))),
    asserta(system:term_expansion((:- use_module(library(semweb/X), Opts)),
                                  (:- use_module(library(LocalDir/X), Opts)))).
fix_load_path.

:- assert_search_paths.
:- fix_load_path.
