/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 1985-2013, University of Amsterdam
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


:- module(rdf_http_plugin, []).
:- use_module(library(http/http_open)).
:- use_module(library(http/http_header)).
:- use_module(library(semweb/rdf_db), []). % we define hooks for this
:- use_module(library(date)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(option)).

/** <module> RDF HTTP Plugin

This module allows loading data into   the semantic web library directly
from an HTTP server. The following example  loads the RDF core data into
the RDF database.

    ==
    :- use_module(library(semweb/rdf_db)).
    :- use_module(library(semweb/rdf_http_plugin)).

	...,
	rdf_load('http://www.w3.org/1999/02/22-rdf-syntax-ns')
    ==
*/

:- multifile(error:has_type/2).

error:has_type(rdf_format, Term):-
  error:has_type(oneof([nquads,ntriples,rdfa,trig,turtle,xml]), Term).

:- multifile
	rdf_content_type/2.		% Allow defining additional
					% content-type mappings.

:- multifile
	rdf_db:rdf_open_hook/8,
	rdf_db:url_protocol/1.

rdf_db:url_protocol(http).
rdf_db:url_protocol(https).


%%	rdf_extra_headers(-List)
%
%	Send extra headers with the request. Note that, although we also
%	process RDF embedded in HTML, we do  not explicitely ask for it.
%	Doing so causes some   (e.g., http://w3.org/2004/02/skos/core to
%	reply with the HTML description rather than the RDF).

rdf_extra_headers(
  [cert_verify_hook(ssl_verify),request_header('Accept'=AcceptValue)],
  Options
):-
  option(format(Format), Options, _VAR),
  rdf_accept_header_value(Format, AcceptValue).


%%	rdf_db:rdf_open_hook(+Scheme, +URL, +HaveModified,
%%			     -Stream, -Cleanup, -Modified, -Format,
%%			     +Options) is semidet.
%
%	Load hook implementation for HTTP(S) URLs.
%
%	@arg HaveModified is bound to a timestamp (number) if we already
%	     have a copy and that copy was modified at HaveModified.
%	@arg Modified is bound to =unknown=, =not_modified= or a
%	     timestamp.

rdf_db:rdf_open_hook(https, SourceURL, HaveModified, Stream, Cleanup,
		     Modified, Format, Options) :-
	rdf_db:rdf_open_hook(http, SourceURL, HaveModified, Stream, Cleanup,
			     Modified, Format, Options).
rdf_db:rdf_open_hook(http, SourceURL, HaveModified, Stream, Cleanup,
		     Modified, Format, Options) :-
	modified_since_header(HaveModified, Header),
	TypeHdr = [ header(content_type, ContentType),
		    header(last_modified, ModifiedText)
		  ],
	rdf_extra_headers(Extra, Options),
	append([Extra, TypeHdr, Header, Options], OpenOptions),
	catch(http_open(SourceURL, Stream0, OpenOptions), E, true),
	(   var(E)
	->  (   open_envelope(ContentType, SourceURL,
			      Stream0, Stream, Format)
	    ->	Cleanup = close(Stream),
		(   nonvar(ModifiedText),
		    parse_time(ModifiedText, ModifiedStamp)
		->  Modified = last_modified(ModifiedStamp)
		;   Modified = unknown
		)
	    ;	close(Stream0),
		domain_error(content_type, ContentType)
	    )
	;   subsumes_term(error(_, context(_, status(304, _))), E)
	->  Modified = not_modified,
	    Cleanup = true
	;   throw(E)
	).

:- public ssl_verify/5.

%%	ssl_verify(+SSL, +ProblemCert, +AllCerts, +FirstCert, +Error)
%
%	Currently we accept  all  certificates.

ssl_verify(_SSL,
	   _ProblemCertificate, _AllCertificates, _FirstCertificate,
	   _Error).

%%	modified_since_header(+LastModified, -ExtraHeaders) is det.
%
%	Add an =|If-modified-since|= if we have a version with the given
%	time-stamp.

modified_since_header(HaveModified, []) :-
	var(HaveModified), !.
modified_since_header(HaveModified,
		      [ request_header('If-modified-since' =
				       Modified)
		      ]) :-
	http_timestamp(HaveModified, Modified).

%%	open_envelope(+ContentType, +SourceURL, +Stream0, -Stream,
%%		      ?Format) is semidet.
%
%	Open possible envelope formats.

open_envelope('application/x-gzip', SourceURL, Stream0, Stream, Format) :-
	rdf_db:rdf_storage_encoding(_, gzip), !,
	(   var(Format)
	->  file_name_extension(BaseURL, _GzExt, SourceURL),
	    file_name_extension(_, Ext, BaseURL),
	    rdf_db:rdf_file_type(Ext, Format)
	;   true
	),
	rdf_zlib_plugin:zopen(Stream0, Stream, []).
open_envelope(_, _, Stream, Stream, Format) :-
	nonvar(Format), !.
open_envelope(ContentType, SourceURL, Stream, Stream, Format) :-
	major_content_type(ContentType, Major),
	(   rdf_content_type(Major, Format)
	->  true
	;   Major == 'text/plain'	% server is not properly configured
	->  file_name_extension(_, Ext, SourceURL),
	    rdf_db:rdf_file_type(Ext, Format)
	).

major_content_type(ContentType, Major) :-
	sub_atom(ContentType, Pre, _, _, (;)), !,
	sub_atom(ContentType, 0, Pre, _, Major).
major_content_type(Major, Major).

%%	rdf_content_type(+ContentType, -URL) is semidet.
%
%	Deduce the RDF encoding from the   mime-type.  This predicate is
%	defined as multifile such that the user can associate additional
%	content types to RDF formats.
%
%	@bug	The turtle parser only parses a subset of n3.

:- multifile
	rdf_content_type/2.

rdf_content_type('text/rdf+n3',		  turtle).	% Bit dubious
rdf_content_type('text/html',		  xhtml).
rdf_content_type('application/xhtml+xml', xhtml).
rdf_content_type('application/x-gzip',	  gzip).
rdf_content_type(MediaType, Format):- rdf_content_type(MediaType, _, Format).


%! rdf_accept_header_value(?Format:rdf_format, -AcceptValue:atom) is det.

rdf_accept_header_value(Format, AcceptValue):-
  findall(
    AcceptValue,
    (
      rdf_content_type(MediaType, QValue0, Format0),
      (   Format == Format0
      ->  QValue = 1.0
      ;   QValue = QValue0
      ),
      format(atom(AcceptValue), '~a;q=~3f', [MediaType,QValue])
    ),
    AcceptValues
  ),
  atomic_list_concat(['*/*;q=0.001'|AcceptValues], ',', AcceptValue).


%! rdf_content_type(
%!   ?MediaType:atom,
%!   ?QualityValue:between(0.0,1.0),
%!   ?Format:rdf_format
%! ) is nondet.
%
% ### Quality values
%
% Quality values are intended to be used in accordance with RFC 2616.
%
% Quality values are determined based on the following criteria:
%
%   | **Quality value** | **Reason**                   |
%   | 0.45              | Official content type        |
%   | 0.45              | Specific for RDF content     |
%   | 0.05              | Inofficial content type      |
%   | 0.05              | Not specific for RDF content |
%
% For example, `text/tutle` has quality value `0.9` because it is
% an official content type that is RDF-specific.
%
% @see Discussion http://richard.cyganiak.de/blog/2008/03/what-is-your-rdf-browsers-accept-header/
% @see N-Quadruples http://www.w3.org/ns/formats/N-Quads
% @see N-Triples http://www.w3.org/ns/formats/N-Triples
% @see N3 http://www.w3.org/ns/formats/N3
% @see RDFa http://www.w3.org/ns/formats/RDFa
% @see TriG http://www.w3.org/ns/formats/TriG
% @see Turtle http://www.w3.org/ns/formats/Turtle
% @see XML/RDF http://www.w3.org/ns/formats/RDF_XML

rdf_content_type('application/n-quads', 0.9, nquads).
rdf_content_type('application/n-triples', 0.9, ntriples).
rdf_content_type('application/rdf', 0.5, xml).
rdf_content_type('application/rdf+turtle', 0.5, turtle).
rdf_content_type('application/rdf+xml', 0.5, xml).
rdf_content_type('application/rss+xml', 0.1, xml).
rdf_content_type('application/trig', 0.9, trig).
rdf_content_type('application/turtle', 0.5, turtle).
rdf_content_type('application/x-trig', 0.5, trig).
rdf_content_type('application/x-turtle', 0.5, turtle).
rdf_content_type('application/xhtml+xml', 0.2, rdfa).
rdf_content_type('application/xml', 0.1, xml).
rdf_content_type('text/html', 0.1, rdfa).
rdf_content_type('text/n3', 0.9, n3).
rdf_content_type('text/rdf', 0.5, xml).
rdf_content_type('text/rdf+n3', 0.5, n3).
rdf_content_type('text/rdf+xml', 0.9, xml).
rdf_content_type('text/turtle', 0.9, turtle).
rdf_content_type('text/xml', 0.5, xml).
