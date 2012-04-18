/*  Part of XPCE --- The SWI-Prolog GUI toolkit

    Author:        Jan Wielemaker and Anjo Anjewierden
    E-mail:        jan@swi.psy.uva.nl
    WWW:           http://www.swi.psy.uva.nl/projects/xpce/
    Copyright (C): 1985-2002, University of Amsterdam

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

:- module(pce_graph,
	  [			% From, To
	  ]).
:- use_module(library(broadcast)).

:- use_module(library(pce)).
:- require([ forall/2
	   , term_variables/2
	   , random/3
	   , term_to_atom/2
	   ]).


:- pce_begin_class(graph_viewer, frame).

variable(graph,  any*, get, "Associated graph").

initialise(GV, Id:[any]) :->
	"Create graph-viewer"::
	send_super(GV, initialise, 'Graph Viewer'),
	send(GV, append, new(P, picture)),
	send(new(D, dialog), below, P),
	fill_dialog(D),
	send(GV, slot, graph, Id),
	listen(GV, graph(Id),
	       in_pce_thread(identify(graph(Id), GV))),
	listen(GV, graph(Id, Action),
	       in_pce_thread(action(Action, GV))).

unlink(GV) :->
	unlisten(GV),
	send_super(GV, unlink).

identify(graph(Graph), GV) :-		% See whether the graph is monitored
	get(GV, graph, Graph).

action(add_node(N), GV) :-
	get(GV, node, N, _Node).
action(add_edge(C,P), GV) :-
	send(GV, display_arc, C, P).
action(del_edge(C,P), GV) :-
	send(GV, delete_arc, C, P).
action(reset, GV) :-
	send(GV, clear).

fill_dialog(D) :-
	new(Frame, D?frame),

	send(D, append, label(reporter)),

	send(D, append, button(generate, message(Frame, generate_from_atom,
					       D?generator_member?selection))),
	send(D, append, text_item(generator, '',
				  message(D?generate_member, execute)), right),

	send(D, append, button(quit, message(Frame, destroy))),
	send(D, append, button(clear, message(Frame, clear))),
	send(D, append, button(postscript, message(Frame, postscript))),
	send(D, append, button(layout, message(Frame, layout))).


clear(F) :->
	"Clear the diagram"::
	get(F, member, picture, P),
	send(P, clear).


layout(F) :->
	"Run graph layout"::
	get(F, member, picture, P),
	(   get(P?graphicals, head, Head)
	->  send(Head, layout)
	;   send(F, report, error, 'No graph to layout')
	).


:- pce_autoload(finder, library(find_file)).
:- pce_global(@finder, new(finder)).

postscript(F) :->
	"Create PostScript in file"::
	get(@finder, file, @off, '.eps', FileName),
	get(F, member, picture, Pict),
	new(File, file(FileName)),
	send(File, open, write),
	send(File, append, Pict?postscript),
	send(File, close),
	send(File, done),
	send(F, report, status, 'Saved PostScript in %s', FileName).


generate_from_atom(F, Generator:name) :->
	"Create graph using generator"::
	(   term_to_atom(Term, Generator),
	    term_variables(Term, [From, To])
	->  send(F, generate, From-To, Term)
	;   send(F, report, error,
		 'Generator should be a Prolog goal with two variables')
	).


generate(F, Templ:prolog, Goal:prolog) :->
	"Create graph using generator"::
	send(F, clear),
	Templ = (From-To),
	catch(forall(user:Goal,
		     send(F, display_arc, From, To)),
	      E,
	      (	  message_to_string(E, Error),
		  send(F, report, error, Error),
		  fail
	      )),
	send(F, layout),
	(   From = '$VAR'('From'),
	    To = '$VAR'('To'),
	    format(string(Label), '~W', [Goal, [quoted(true), numbervars(true)]]),
	    send(F, label, Label),
	    fail
	;   true
	).


:- pce_global(@graph_link, new(link(link, link, line(0,0,0,0,second)))).

display_arc(F, From:name, To:name) :->
	"Display arc From -> To"::
	get(F, node, From, NF),
	get(F, node, To, TF),
	send(NF, connect, TF, @graph_link).


delete_arc(F, From:name, To:name) :->
	"Delete arc From -> To"::
	get(F, node, From, NF),
	get(F, node, To, TF),
	(   get(NF, connected, TF, @graph_link, C)
	->  send(C, destroy)
	;   true
	).

node(F, Name:name, Node:graph_node) :<-
	"Get (create) node with specified name"::
	get(F, member, picture, Picture),
	(   get(Picture, member, Name, Node)
	->  true
	;   get(Picture, visible, area(X, Y, W, H)),
	    MX is X + W,
	    MY is Y + H,
	    random(X, MX, NX),
	    random(Y, MY, NY),
	    send(Picture, display, new(Node, graph_node(Name)), point(NX, NY))
	).


:- pce_end_class.


:- pce_begin_class(graph_node(name), device).

handle(w/2, 0, link, link).

initialise(Node, Name:name) :->
	"Create from name"::
	send(Node, send_super, initialise),
	send(Node, display, circle(5), point(-2, -2)),
	send(Node, display, new(T, text(Name, center))),
	send(T, center, point(0, 10)),
	send(Node, send_super, name, Name).

name(Node, Name:name) :->
	"Change name of a node"::
	get(Node, member, text, Text),
	send(Text, string, Name),
	send(Node, send_super, name, Name).

:- pce_global(@graph_node_recogniser, make_graph_node_recogniser).

make_graph_node_recogniser(R) :-
	new(R, move_gesture(left)),
	send(R, condition,
	     ?(@event?position, distance, point(0,0)) < 5).


event(Node, Ev:event) :->
	"Make it movable"::
	(   send(@graph_node_recogniser, event, Ev)
	->  true
	;   send(Node, send_super, event, Ev)
	).

:- pce_end_class.
