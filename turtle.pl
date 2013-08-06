/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2004-2013, University of Amsterdam
			      VU University Amsterdam

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

    As a special exception, if you link this library with other files,
    compiled with a Free Software compiler, to produce an executable, this
    library does not by itself cause the resulting executable to be covered
    by the GNU General Public License. This exception does not however
    invalidate any other reasons why the executable file might be covered by
    the GNU General Public License.
*/

:- module(turtle,
	  [ rdf_load_turtle/3,			% +Input, -Triples, +Options
	    rdf_read_turtle/3,			% +Input, -Triples, +Options
	    rdf_process_turtle/3,		% +Input, :OnObject, +Options

	    rdf_save_turtle/2,			% +File, +Options
	    rdf_save_canonical_turtle/2		% +File, +Options
	  ]).
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdf_turtle_write)). % re-exports
:- use_module(library(uri)).
:- use_module(library(http/http_open)).

:- meta_predicate
	rdf_process_turtle(+,2,+).

:- predicate_options(rdf_load_turtle/3, 3,
		     [pass_to(rdf_read_turtle/3, 3)]).
:- predicate_options(rdf_process_turtle/3, 3,
		     [ anon_prefix(atom),
		       base_uri(atom),
		       base_used(-atom),
		       db(atom),
		       error_count(-integer),
		       namespaces(-list),
		       on_error(oneof([warning,error])),
		       prefixes(-list),
		       resources(oneof([uri,iri]))
		     ]).
:- predicate_options(rdf_read_turtle/3, 3,
		     [ anon_prefix(atom),
		       base_uri(atom),
		       base_used(-atom),
		       db(atom),
		       error_count(-integer),
		       namespaces(-list),
		       on_error(oneof([warning,error])),
		       prefixes(-list),
		       resources(oneof([uri,iri]))
		     ]).

:- use_foreign_library(foreign(turtle)).
:- public				% used by the writer
	turtle_pn_local/1,
	turtle_write_quoted_string/2,
	turtle_write_uri/2.

/** <module> Turtle: Terse RDF Triple Language

This module implements the Turtle  language   for  representing  the RDF
triple model as defined by Dave Beckett  from the Institute for Learning
and Research Technology University of Bristol  and later standardized by
the W3C RDF working group.

This module acts as a plugin to   rdf_load/2,  for processing files with
one of the extensions =|.ttl|= or =|.n3|=.

@see	http://www.w3.org/TR/turtle/ (used Candidate Recommendation 19
	February 2013)
*/

%%	rdf_read_turtle(+Input, -Triples, +Options)
%
%	Read a stream or file into a set of triples of the format
%
%		rdf(Subject, Predicate, Object)
%
%	The representation is consistent with the SWI-Prolog RDF/XML
%	and ntriples parsers.  Provided options are:
%
%		* base_uri(+BaseURI)
%		Initial base URI.  Defaults to file://<file> for loading
%		files.
%
%		* anon_prefix(+Prefix)
%		Blank nodes are generated as <Prefix>1, <Prefix>2, etc.
%		If Prefix is not an atom blank nodes are generated as
%		node(1), node(2), ...
%
%		* resources(URIorIRI)
%		Officially, Turtle resources are IRIs.  Quite a
%		few applications however send URIs.  By default we
%		do URI->IRI mapping because this rarely causes errors.
%		To force strictly conforming mode, pass =iri=.
%
%		* prefixes(-Pairs)
%		Return encountered prefix declarations as a
%		list of Alias-URI
%
%		* namespaces(-Pairs)
%		Same as prefixes(Pairs).  Compatibility to rdf_load/2.
%
%		* base_used(-Base)
%		Base URI used for processing the data.  Unified to
%		[] if there is no base-uri.
%
%		* on_error(+ErrorMode)
%		In =warning= (default), print the error and continue
%		parsing the remainder of the file.  If =error=, abort
%		with an exception on the first error encountered.
%
%		* error_count(-Count)
%		If on_error(warning) is active, this option cane be
%		used to retrieve the number of generated errors.
%
%	@param	Input is one of stream(Stream), atom(Atom), a =http=,
%		=https= or =file= url or a filename specification as
%		accepted by absolute_file_name/3.

rdf_read_turtle(In, Triples, Options) :-
	base_uri(In, BaseURI, Options),
	setup_call_cleanup(
	    ( open_input(In, Stream, Close),
	      create_turtle_parser(Parser, Stream,
				   [ base_uri(BaseURI)
				   | Options
				   ])
	    ),
	    ( turtle_parse(Parser, Triples,
			   [ parse(document)
			   | Options
			   ]),
	      post_options(Parser, Options)
	    ),
	    ( destroy_turtle_parser(Parser),
	      call(Close)
	    )).

%%	rdf_load_turtle(+Input, -Triples, +Options)
%
%	@deprecated Use rdf_read_turtle/3

rdf_load_turtle(Input, Triples, Options) :-
	rdf_read_turtle(Input, Triples, Options).


%%	rdf_process_turtle(+Input, :OnObject, +Options) is det.
%
%	Process Turtle input from Input, calling OnObject with a list of
%	triples. Options is the same as for rdf_load_turtle/3.
%
%	Errors encountered are sent to  print_message/2, after which the
%	parser tries to recover and parse the remainder of the data.

rdf_process_turtle(In, OnObject, Options) :-
	base_uri(In, BaseURI, Options),
	option(graph(Graph), Options, BaseURI),
	setup_call_cleanup(
	    ( open_input(In, Stream, Close),
	      create_turtle_parser(Parser, Stream, Options)
	    ),
	    ( process_turtle(Parser, Stream, OnObject, Graph,
			     [ parse(statement)
			     ]),
	      post_options(Parser, Options)
	    ),
	    ( destroy_turtle_parser(Parser),
	      call(Close)
	    )).

post_options(Parser, Options) :-
	prefix_option(Parser, Options),
	namespace_option(Parser, Options),
	base_option(Parser, Options),
	error_option(Parser, Options).

prefix_option(Parser, Options) :-
	(   option(prefixes(Pairs), Options)
	->  turtle_prefixes(Parser, Pairs)
	;   true
	).
namespace_option(Parser, Options) :-
	(   option(namespaces(Pairs), Options)
	->  turtle_prefixes(Parser, Pairs)
	;   true
	).
base_option(Parser, Options) :-
	(   option(base_used(Base), Options)
	->  turtle_base(Parser, Base)
	;   true
	).
error_option(Parser, Options) :-
	(   option(error_count(Count), Options)
	->  turtle_error_count(Parser, Count)
	;   true
	).


process_turtle(_Parser, Stream, _OnObject, _Graph, _Options) :-
	at_end_of_stream(Stream), !.
process_turtle(Parser, Stream, OnObject, Graph, Options) :-
	line_count(Stream, LineNo),
	turtle_parse(Parser, Triples,
		     [ parse(statement)
		     | Options
		     ]),
	call(OnObject, Triples, Graph:LineNo),
	process_turtle(Parser, Stream, OnObject, Graph, Options).


%%	open_input(+Input, -Stream, -Close) is det.
%
%	Open given input.
%
%	@param  Close goal to undo the open action
%	@tbd	Synchronize with input handling of rdf_db.pl.
%	@error	existence_error, permission_error

open_input(stream(Stream), Stream, Close) :- !,
	stream_property(Stream, encoding(Old)),
	(   unicode_encoding(Old)
	->  Close = true
	;   set_stream(Stream, encoding(utf8)),
	    Close = set_stream(Stream, encoding(Old))
	).
open_input(Stream, Stream, Close) :-
	is_stream(Stream), !,
	open_input(stream(Stream), Stream, Close).
open_input(atom(Atom), Stream, close(Stream)) :- !,
	atom_to_memory_file(Atom, MF),
	open_memory_file(MF, read, Stream, [free_on_close(true)]).
open_input(URL, Stream, close(Stream)) :-
	(   sub_atom(URL, 0, _, _, 'http://')
	;   sub_atom(URL, 0, _, _, 'https://')
	), !,
	http_open(URL, Stream, []),
	set_stream(Stream, encoding(utf8)).
open_input(URL, Stream, close(Stream)) :-
	uri_file_name(URL, Path), !,
	open(Path, read, Stream, [encoding(utf8)]).
open_input(File, Stream, close(Stream)) :-
	absolute_file_name(File, Path,
			   [ access(read),
			     extensions([ttl, ''])
			   ]),
	open(Path, read, Stream, [encoding(utf8)]).

unicode_encoding(utf8).
unicode_encoding(wchar_t).
unicode_encoding(unicode_be).
unicode_encoding(unicode_le).

%%	base_uri(+Input, -BaseURI, +Options)
%
%	Determine the base uri to use for processing.

base_uri(_Input, BaseURI, Options) :-
	option(base_uri(BaseURI), Options), !.
base_uri(_Input, BaseURI, Options) :-
	option(graph(BaseURI), Options), !.
base_uri(stream(Input), BaseURI, _Options) :-
	stream_property(Input, file_name(Name)), !,
	name_uri(Name, BaseURI).
base_uri(Stream, BaseURI, Options) :-
	is_stream(Stream), !,
	base_uri(stream(Stream), BaseURI, Options).
base_uri(Name, BaseURI, _Options) :-
	atom(Name), !,
	name_uri(Name, BaseURI).
base_uri(_, 'http://www.example.com/', _).

name_uri(Name, BaseURI) :-
	uri_is_global(Name), !,
	uri_normalized(Name, BaseURI).
name_uri(Name, BaseURI) :-
	uri_file_name(BaseURI, Name).


		 /*******************************
		 *	    WRITE SUPPORT	*
		 *******************************/

%%	turtle_pn_local(+Atom:atom) is semidet.
%
%	True if Atom is a  valid   Turtle  _PN_LOCAL_ name. The PN_LOCAL
%	name is what can follow the : in  a resource. In the new Turtle,
%	this can be anything and this   function becomes meaningless. In
%	the old turtle, PN_LOCAL is defined   similar (but not equal) to
%	an XML name. This predicate  is   used  by  rdf_save_turtle/2 to
%	write files such that can be read by old parsers.
%
%	@see xml_name/2.

%%	turtle_write_quoted_string(+Out, +Value, ?WriteLong) is det.
%
%	Write Value (an atom)  as  a   valid  Turtle  string.  WriteLong
%	determines wether the string is written   as a _short_ or _long_
%	string.  It takes the following values:
%
%	  * true
%	  Use Turtle's long string syntax. Embeded newlines and
%	  single or double quotes are are emitted verbatim.
%	  * false
%	  Use Turtle's shotr string syntax.
%	  * Var
%	  If WriteLong is unbound, this predicate uses long syntax
%	  if newlines appear in the string and short otherwise.  WriteLong
%	  is unified with the decision taken.

%%	turtle_write_quoted_string(+Out, +Value) is det.
%
%	Same as turtle_write_quoted_string(Out, Value, false), writing a
%	string with only a single =|"|=.   Embedded newlines are escapes
%	as =|\n|=.

turtle_write_quoted_string(Out, Text) :-
	turtle_write_quoted_string(Out, Text, false).

%%	turtle_write_uri(+Out, +Value) is det.
%
%	Write a URI as =|<...>|=


		 /*******************************
		 *	    RDF-DB HOOK		*
		 *******************************/

:- multifile
	rdf_db:rdf_load_stream/3,
	rdf_db:rdf_file_type/2.

%%	rdf_db:rdf_load_stream(+Format, +Stream, :Options)
%
%	(Turtle clauses)

rdf_db:rdf_load_stream(turtle, Stream, Options) :-
	load_turtle_stream(Stream, Options).
rdf_db:rdf_load_stream(trig, Stream, Options) :-
	load_turtle_stream(Stream, Options).

load_turtle_stream(Stream, _Module:Options) :-
	rdf_db:graph(Options, Graph),
	atom_concat('__', Graph, BNodePrefix),
	rdf_transaction((  rdf_process_turtle(Stream, assert_triples,
					      [ anon_prefix(BNodePrefix)
					      | Options
					      ]),
			   rdf_set_graph(Graph, modified(false))
			),
			parse(Graph)).

assert_triples([], _).
assert_triples([H|T], Location) :-
	assert_triple(H, Location),
	assert_triples(T, Location).

assert_triple(rdf(S,P,O), Location) :-
	rdf_assert(S,P,O,Location).
assert_triple(rdf(S,P,O,G), _) :-
	rdf_assert(S,P,O,G).


rdf_db:rdf_file_type(ttl,  turtle).
rdf_db:rdf_file_type(n3,   turtle).	% not really, but good enough
rdf_db:rdf_file_type(trig, trig).
