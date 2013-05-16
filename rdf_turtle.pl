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

:- module(rdf_turtle,
	  [ rdf_load_turtle/3,		% +Input, -Triples, +Options
	    rdf_read_turtle/3,		% +Input, -Triples, +Options
	    rdf_process_turtle/3	% +Input, :OnObject, +Options
	  ]).
:- use_module(library(semweb/turtle)).	% re-export

/** <module> Turtle reader

This module provides a parser for the Turtle language. The parser itself
has been moved to library(semweb/turtle),   which  encapsulates a single
foreign object that provides the common Turtle infrastructure.

@deprecated	The Turtle reading and writing facilities have been merged
		into library(semweb/turtle).
*/
