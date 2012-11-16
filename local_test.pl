/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Adjust paths to allow running the tests without installing.  This file
is normally included into the test scripts.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

:- asserta(user:file_search_path(foreign, '../sgml')).
:- asserta(user:file_search_path(foreign, '../clib')).
:- asserta(user:file_search_path(foreign, '../zlib')).
:- asserta(user:file_search_path(library, '../sgml')).
:- asserta(user:file_search_path(library, '../clib')).
:- asserta(user:file_search_path(library, '../zlib')).
:- asserta(user:file_search_path(library, '../RDF')).
:- asserta(user:file_search_path(library, '../plunit')).
:- asserta(user:file_search_path(library, '..')).
:- asserta(user:file_search_path(foreign, '.')).

fix_load_path :-
	prolog_load_context(directory, Dir),
	file_base_name(Dir, LocalDir),
	LocalDir \== semweb, !,
	asserta(system:term_expansion((:- use_module(library(semweb/X))),
				      (:- use_module(library(LocalDir/X))))),
	asserta(system:term_expansion((:- use_module(library(semweb/X), Opts)),
				      (:- use_module(library(LocalDir/X), Opts)))).
fix_load_path.

:- fix_load_path.
