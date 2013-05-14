/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2013, VU University Amsterdam

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

:- module(rdf_ntriples,
	  [ rdf_read_ntriples/3,	% +Input, -Triples, +Options
	    rdf_process_ntriples/3,	% +Input, :CallBack, +Options

	    read_ntriple/2		% +Stream, -Triple
	  ]).
:- use_module(library(record)).
:- use_module(library(uri)).
:- use_module(library(option)).
:- use_module(library(http/http_open)).
:- use_module(library(semweb/rdf_db)).
:- use_foreign_library(foreign(ntriples)).

/** <module> Process files in the RDF N-Triples format

The library(semweb/rdf_ntriples) provides a  fast   reader  for  the RDF
N-Triples format. N-Triples is  a  simple   format,  originally  used to
support the W3C RDF test suites. The   current  format has been extended
and is a subset of the Turtle format (see library(semweb/rdf_turtle)).

The    API    of    this    library      is    almost    identical    to
library(semweb/rdf_turtle).  This  module   provides    a   plugin  into
rdf_load/2, making this predicate support the format =ntriples=.

@see http://www.w3.org/TR/n-triples/
*/

:- predicate_options(rdf_read_ntriples/3, 3,
		     [ anon_prefix(any), % atom or node(_)
		       base_uri(atom),
		       error_count(-integer),
		       on_error(oneof([warning,error]))
		     ]).
:- predicate_options(rdf_process_ntriples/3, 3,
		     [ graph(atom),
		       pass_to(rdf_read_ntriples/3, 3)
		     ]).

:- meta_predicate
	rdf_process_ntriples(+,2,+).


%%	read_ntriple(+Stream, -Triple) is det.
%
%	Read the next triple from Stream as   Triple. Stream must have a
%	byte-oriented encoding and must contain pure ASCII text.
%
%	@param	Triple is a term triple(Subject,Predicate,Object).
%		Arguments follow the normal conventions of the RDF
%		libraries.  NodeID elements are mapped to node(Id).
%		If end-of-file is reached, Triple is unified with
%		=end_of_file=.
%	@error	syntax_error(Message) on syntax errors

:- record nt_state(anon_prefix,
		   graph,
		   on_error:oneof([warning,error])=warning,
		   error_count=0).


%%	rdf_read_ntriples(+Input, -Triples, +Options)
%
%	True when Triples is a list of triples from Input.  Options:
%
%	  * anon_prefix(+AtomOrNode)
%	  Prefix nodeIDs with this atom.  If AtomOrNode is the term
%	  node(_), bnodes are returned as node(Id).
%	  * base_uri(+Atom)
%	  Defines the default anon_prefix as __<baseuri>_
%	  * on_error(Action)
%	  One of =warning= (default) or =error=
%	  * error_count(-Count)
%	  If =on_error= is =warning=, unify Count with th number of
%	  errors.

rdf_read_ntriples(Input, Triples, Options) :-
	setup_call_cleanup(
	    open_input(Input, Stream, Close),
	    (	init_state(Input, Options, State0),
		read_ntriples(Stream, Triples, State0, State)
	    ),
	    Close),
	option(error_count(Count), Options, _),
	nt_state_error_count(State, Count).

%%	rdf_process_ntriples(+Input, :CallBack, +Options)
%
%	Call-back interface, compatible with the   other triple readers.
%	In  addition  to  the  options  from  rdf_read_ntriples/3,  this
%	processes the option graph(Graph).
%
%	@param	CallBack is called as call(CallBack, Triples, Graph),
%		where Triples is a list holding a single rdf(S,P,O)
%		triple.  Graph is passed from the =graph= option and
%		unbound if this option is omitted.

rdf_process_ntriples(Input, CallBack, Options) :-
	setup_call_cleanup(
	    open_input(Input, Stream, Close),
	    (	init_state(Input, Options, State0),
		process_ntriple(Stream, CallBack, State0, State)
	    ),
	    Close),
	option(error_count(Count), Options, _),
	nt_state_error_count(State, Count).


%%	read_ntriples(+Stream, -Triples, +State0, -State)

read_ntriples(Stream, Triples, State0, State) :-
	read_triple(Stream, Triple0, State0, State1),
	(   Triple0 == end_of_file
	->  Triples = [],
	    State = State1
	;   map_nodes(Triple0, Triple, State1, State2),
	    Triples = [Triple|More],
	    read_ntriples(Stream, More, State2, State)
	).

%%	process_ntriple(+Stream, :CallBack, +State0, -State)

process_ntriple(Stream, CallBack, State0, State) :-
	read_triple(Stream, Triple0, State0, State1),
	(   Triple0 == end_of_file
	->  State = State1
	;   map_nodes(Triple0, Triple, State1, State2),
	    nt_state_graph(State2, Graph),
	    call(CallBack, [Triple], Graph),
	    process_ntriple(Stream, CallBack, State2, State)
	).

%%	read_triple(+Stream, -Triple, +State0, -State) is det.
%
%	True when Triple is the next triple on Stream.  May increment
%	the error count on State.

read_triple(Stream, Triple, State0, State) :-
	nt_state_on_error(State0, error), !,
	read_ntriple(Stream, Triple),
	State = State0.
read_triple(Stream, Triple, State0, State) :-
	catch(read_ntriple(Stream, Triple), E, true),
	(   var(E)
	->  State = State0
	;   print_message(warning, E),
	    nt_state_error_count(State0, EC0),
	    EC is EC0+1,
	    set_error_count_of_nt_state(EC, State0, State1),
	    read_triple(Stream, Triple, State1, State)
	).

map_nodes(triple(S0,P0,O0), rdf(S,P,O), State0, State) :-
	map_node(S0, S, State0, State1),
	map_node(P0, P, State1, State2),
	map_node(O0, O, State2, State).

map_node(node(NodeId), BNode, State, State) :-
	nt_state_anon_prefix(State, Prefix),
	atom(Prefix), !,
	atom_concat(Prefix, NodeId, BNode).
map_node(Node, Node, State, State).


%%	open_input(+Input, -Stream, -Close) is det.

open_input(stream(Stream), Stream, true) :- !,
	stream_property(Stream, encoding(Old)),
	(   n3_encoding(Old)
	->  true
	;   domain_error(ntriples_encoding, Old)
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
			     extensions([nt, ntriples, ''])
			   ]),
	open(Path, read, Stream, [encoding(utf8)]).

n3_encoding(octet).
n3_encoding(ascii).
n3_encoding(iso_latin_1).
n3_encoding(utf8).
n3_encoding(text).

%%	init_state(+Input, +Options, -State) is det.

init_state(In, Options, State) :-
	(   option(base_uri(BaseURI), Options)
	->  true
	;   In = stream(_)
	->  BaseURI = []
	;   In = atom(_)
	->  BaseURI = []
	;   uri_is_global(In),
	    \+ is_absolute_file_name(In)	% Avoid C:Path in Windows
	->  uri_normalized(In, BaseURI)
	;   uri_file_name(BaseURI, In)
	),
	(   option(anon_prefix(Prefix), Options)
	->  true
	;   BaseURI == []
	->  Prefix = '__bnode'
	;   atom_concat('__', BaseURI, Prefix)
	),
	option(on_error(OnError), Options, warning),
	rdf_db:graph(Options, Graph),
	make_nt_state([ anon_prefix(Prefix),
			on_error(OnError),
			graph(Graph)
		      ], State).


		 /*******************************
		 *	    RDF-DB HOOK		*
		 *******************************/

:- multifile
	rdf_db:rdf_load_stream/3,
	rdf_db:rdf_file_type/2.

%%	rdf_db:rdf_load_stream(+Format, +Stream, :Options) is semidet.
%
%	Plugin rule that supports loading the =ntriples= format.

rdf_db:rdf_load_stream(ntriples, Stream, _Module:Options) :-
	rdf_db:graph(Options, Graph),
	rdf_transaction((  rdf_process_ntriples(Stream, assert_triples, Options),
			   rdf_set_graph(Graph, modified(false))
			),
			parse(Graph)).

assert_triples([], _).
assert_triples([rdf(S,P,O)|T], Graph) :-
	rdf_assert(S,P,O,Graph),
	assert_triples(T, Graph).

%%	rdf_db:rdf_file_type(+Extension, -Format)
%
%	Bind the ntriples reader to files   with the extensions =nt= and
%	=ntriples=.

rdf_db:rdf_file_type(nt,       ntriples).
rdf_db:rdf_file_type(ntriples, ntriples).
