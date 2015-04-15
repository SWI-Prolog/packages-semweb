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

:- module(
  rdf_serialization_format,
  [
    rdf_accept_header_value/2 % ?Format:rdf_format
                              % -AcceptValue:atom
  ]
).
:- use_module(library(lists)).

/** <module> RDF Serialization Format

This module stores information regarding the various
RDF serialization formats.
This information is used to set the Accept header of HTTP requests
for RDF data.

@author Wouter Beek
@author Jan Wielemaker
@version 2015/04/15
*/

:- multifile(error:has_type/2).

error:has_type(rdf_format, Term):-
  error:has_type(oneof([nquads,ntriples,rdfa,trig,turtle,xml]), Term).


%! rdf_accept_header_value(?Format:rdf_format, -AcceptValue:atom) is det.

rdf_accept_header_value(Format, AcceptValue):-
  findall(
    AcceptValue,
    (
      rdf_serialization(_, Format0, MediaTypes, _),
      member(QValue0-MediaType, MediaTypes),
      (   Format == Format0
      ->  QValue = 1.0
      ;   QValue = QValue0
      ),
      format(
        atom(AcceptValue),
        '~a/~a;q=~f',
        [MediaType.type,MediaType.subtype,QValue]
      )
    ),
    AcceptValues
  ),
  atomic_list_concat(['*/*;q=0.001'|AcceptValues], ',', AcceptValue).


%! rdf_serialization(
%!   ?DefaultFileExtension:atom,
%!   ?Format:rdf_format,
%!   ?MediaTypes:list(pair(between(0.0,1.0),dict)),
%!   ?Uri:atom
%! ) is nondet.
%
% ### Content types
%
% Content types are represented as pairs.
% The first element of the pair is a qvalue, as specified in RFC 2616.
% The second element of the pair is the content type name.
%
% The qvalues are determined based on the following criteria:
%   | **Q-Value** | **Reason**                   |
%   | 0.45        | Official content type        |
%   | 0.45        | Specific for RDF content     |
%   | 0.05        | Inofficial content type      |
%   | 0.05        | Not specific for RDF content |
%
% For example, `text/tutle` has qvalue `0.9` because it is
% an official content type that is RDF-specific.
%
% ### Arguments
%
% @arg DefaultExtension The default extension of the RDF serialization.
%      RDF serializations may have multiple non-default extensions,
%      e.g. `owl` and `xml` for RDF/XML, that are not included here.
% @arg Format The format name that is used by the Semweb library.
% @arg MediaTypes A list of pairs of qvalues and media types.
% @arg Uri The URI at which the serialization is described.
%
% @see http://richard.cyganiak.de/blog/2008/03/what-is-your-rdf-browsers-accept-header/

rdf_serialization(
  nq,
  nquads,
  [0.9-'media-type'{type:application, subtype:"n-quads", parameters:[]}],
  'http://www.w3.org/ns/formats/N-Quads'
).
rdf_serialization(
  nt,
  ntriples,
  [0.9-'media-type'{type:application, subtype:"n-triples", parameters:[]}],
  'http://www.w3.org/ns/formats/N-Triples'
).
rdf_serialization(
  rdf,
  xml,
  [
    0.9-'media-type'{type:text, subtype:"rdf+xml", parameters:[]},
    0.5-'media-type'{type:application, subtype:"rdf+xml", parameters:[]},
    0.5-'media-type'{type:text, subtype:rdf, parameters:[]},
    0.5-'media-type'{type:text, subtype:xml, parameters:[]},
    0.5-'media-type'{type:application, subtype:rdf, parameters:[]},
    0.1-'media-type'{type:application, subtype:"rss+xml", parameters:[]},
    0.1-'media-type'{type:application, subtype:xml, parameters:[]}
  ],
  'http://www.w3.org/ns/formats/RDF_XML'
).
rdf_serialization(
  rdfa,
  rdfa,
  [
    0.2-'media-type'{type:application, subtype:"xhtml+xml", parameters:[]},
    0.1-'media-type'{type:text, subtype:html, parameters:[]}
  ],
  'http://www.w3.org/ns/formats/RDFa'
).
rdf_serialization(
  trig,
  trig,
  [
    0.9-'media-type'{type:application, subtype:trig, parameters:[]},
    0.5-'media-type'{type:application, subtype:"x-trig", parameters:[]}
  ],
  'http://www.w3.org/ns/formats/TriG'
).
rdf_serialization(
  ttl,
  turtle,
  [
    0.9-'media-type'{type:text, subtype:turtle, parameters:[]},
    0.5-'media-type'{type:application, subtype:turtle, parameters:[]},
    0.5-'media-type'{type:application, subtype:"x-turtle", parameters:[]},
    0.5-'media-type'{type:application, subtype:"rdf+turtle", parameters:[]}
  ],
  'http://www.w3.org/ns/formats/Turtle'
).
rdf_serialization(
  n3,
  n3,
  [
    0.9-'media-type'{type:text, subtype:n3, parameters:[]},
    0.5-'media-type'{type:text, subtype:"rdf+n3", parameters:[]}
  ],
  'http://www.w3.org/ns/formats/N3'
).
