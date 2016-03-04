/*  Part of SWI-Prolog

    Author:        Jan Wielemaker and Wouter Beek
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2015-2016, VU University Amsterdam

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

:- module(rdf11_containers,
	  [ rdf_alt/3,				  % ?Alt, -Default, -Others
	    rdf_assert_alt/3,			  % ?Alt, +Default, +Others
	    rdf_assert_alt/4,			  % ?Alt, +Default, +Others, +G
	    rdf_assert_bag/2,			  % ?Bag, +Set
	    rdf_assert_bag/3,			  % ?Bag, +Set, +G
	    rdf_assert_seq/2,			  % ?Seq, +List
	    rdf_assert_seq/3,			  % ?Seq, +List, +G
	    rdf_bag/2,				  % ?Bag, -List
	    rdf_seq/2,				  % ?Seq, -List
	    rdfs_container/2,			  % ?Container, -List
	    rdfs_container_membership_property/1, % ?Property
	    rdfs_container_membership_property/2, % ?Property, ?Number
	    rdfs_member/2,			  % ?Container, ?Member
	    rdfs_nth0/3				  % ?Index, ?Container, ?Member
	  ]).
:- use_module(library(apply)).
:- use_module(library(pairs)).
:- use_module(library(error)).
:- use_module(rdf11).

/** <module> RDF 1.1 Containers

Implementation of the conventional human interpretation of RDF 1.1 containers.

RDF containers are open enumeration structures as opposed to RDF collections or
RDF lists which are closed enumeration structures.
The same resource may appear in a container more than once.
A container may be contained in itself.

---

@author Wouter Beek
@author Jan Wielemaker
@compat RDF 1.1
@see http://www.w3.org/TR/2014/REC-rdf-schema-20140225/#ch_containervocab
@version 2016/01
*/

:- rdf_meta
	rdf_alt(r, -, -),
	rdf_assert_alt(r, r, t),
	rdf_assert_alt(r, r, t, r),
	rdf_assert_bag(r, t),
	rdf_assert_bag(r, t, r),
	rdf_assert_seq(r, t),
	rdf_assert_seq(r, t, r),
	rdf_bag(r, -),
	rdf_seq(r, -),
	rdfs_assert_container(r, t),
	rdfs_assert_container(r, t, r),
	rdfs_assert_container(r, r, t, r),
	rdfs_container(r, -),
	rdfs_container_membership_property(r),
	rdfs_container_membership_property(r,?),
	rdfs_member(r, -).


%%	rdf_alt(+Alt, ?Default, ?Others) is nondet.
%
%	True when Alt is  an  instance   of  rdf:Alt  with  first member
%	Default and remaining members Others.
%
%	Notice that this construct adds no machine-processable semantics
%	but is conventionally used to indicate   to  a human reader that
%	the numerical ordering of the container membership properties of
%	Container is intended to  only   be  relevant  in distinguishing
%	between the first and all non-first members.
%
%	Default denotes the default option to  take when choosing one of
%	the alternatives container in  Container.   Others  denotes  the
%	non-default options that can be chosen from.

rdf_alt(Alt, Default, Others) :-
	rdfs_container(Alt, [Default|Others]).

%%	rdf_assert_alt(?Alt, +Default, +Others:list) is det.
%%	rdf_assert_alt(?Alt, +Default, +Others:list, +Graph) is det.
%
%	Create an rdf:Alt with the given Default and Others. Default and
%	the  members  of  Others  must  be    valid   object  terms  for
%	rdf_assert/3.

rdf_assert_alt(Alt, H, T) :-
	rdf_default_graph(G),
	rdf_assert_alt(Alt, H, T, G).

rdf_assert_alt(Alt, H, T, G) :-
	rdfs_assert_container(Alt, rdf:'Alt', [H|T], G).


%%	rdf_bag(+Bag, -List:list) is nondet.
%
%	True when Bag is an rdf:Bag and   set  is the set values related
%	through container membership properties to Bag.
%
%	Notice that this construct adds no machine-processable semantics
%	but is conventionally used to indicate   to  a human reader that
%	the numerical ordering of the container membership properties of
%	Container is intended to not be significant.

rdf_bag(Bag, List) :-
	rdfs_container(Bag, List).


%%	rdf_assert_bag(?Bag, +Set:list) is det.
%%	rdf_assert_bag(?Bag, +Set:list, +Graph) is det.
%
%	Create an rdf:Bag from the given set   of values. The members of
%	Set must be valid object terms for rdf_assert/3.

rdf_assert_bag(Bag, L) :-
	rdf_default_graph(G),
	rdf_assert_bag(Bag, L, G).

rdf_assert_bag(Bag, L, G) :-
	rdfs_assert_container(Bag, rdf:'Bag', L, G).


%%	rdf_seq(+Seq, -List:list) is nondet.
%
%	True when Seq is an instance of rdf:Seq   and  List is a list of
%	associated values, ordered according to the container membership
%	property used.
%
%	Notice that this construct adds no machine-processable semantics
%	but is conventionally used to indicate   to  a human reader that
%	the numerical ordering of the container membership properties of
%	Container is intended to be significant.

rdf_seq(Seq, L) :-
	rdfs_container(Seq, L).


%%	rdf_assert_seq(?Seq, +List) is det.
%%	rdf_assert_seq(?Seq, +List, +Graph) is det.

rdf_assert_seq(Seq, L) :-
	rdf_default_graph(G),
	rdf_assert_seq(Seq, L, G).
rdf_assert_seq(Seq, L, G) :-
	rdfs_assert_container(Seq, rdf:'Seq', L, G).


%%	rdfs_assert_container(?Container, +Class, +Members, +Graph)

rdfs_assert_container(Container, Class, Members, G) :-
	must_be(list, Members),
	rdf_transaction(rdfs_assert_container_(Container, Class, Members, G)).

rdfs_assert_container_(Container, Class, Members, G) :-
	(var(Container) -> rdf_create_bnode(Container) ; true),
	rdf_assert(Container, rdf:type, Class, G),
	rdfs_assert_members(Members, 1, Container, G).

rdfs_assert_members([], _, _, _).
rdfs_assert_members([H|T], N1, Resource, G) :- !,
	rdf_equal(rdf:'_', Prefix),
	atom_concat(Prefix, N1, P),
	rdf_assert(Resource, P, H, G),
	N2 is N1 + 1,
	rdfs_assert_members(T, N2, Resource, G).


%%	rdfs_container(+Container, -List) is nondet.
%
%	True when List is the  list   of  objects  attached to Container
%	using a container membership property  (rdf:_0, rdf:_1, ...). If
%	multiple objects are connected to the   Container using the same
%	membership  property,  this   predicate    selects   one   value
%	non-deterministically.

rdfs_container(Container, List) :-
	rdf_is_subject(Container), !,
	findall(N-Member, rdfs_member(Container, N, Member), Pairs),
	keysort(Pairs, Sorted),
	group_pairs_by_key(Sorted, GroupedPairs),
	pairs_values(GroupedPairs, Groups),
	maplist(member, List, Groups).
rdfs_container(Container, _) :-
	type_error(rdf_subject, Container).


%%	rdfs_container_membership_property(?Property) is nondet.
%
%	True when Property is a   container membership property (rdf:_1,
%	rdf:_2, ...).

rdfs_container_membership_property(P) :-
	rdfs_container_membership_property(P, _).


%%	rdfs_container_membership_property(?Property, ?Number:nonneg) is nondet.
%
%	True when Property is the Nth container membership property.

rdfs_container_membership_property(P, N) :-
	var(P), !,
	between(1, inf, N),
	rdf_equal(rdf:'_', Prefix),
	atom_concat(Prefix, N, P).
rdfs_container_membership_property(P, N) :-
	atom(P),
	rdf_equal(rdf:'_', Prefix),
	string_concat(Prefix, NumS, P),
	number_string(N, NumS),
	integer(N),
	N >= 0.


%%	rdfs_member(?Container, ?Member) is nondet.
%
%	True if rdf(Container, P, Member) is true   and P is a container
%	membership property.

rdfs_member(Container, Member) :-
	rdfs_member(Container, _, Member).

%%	rdfs_nth0(?Index, ?Container, ?Member) is nondet.
%
%	True if rdf(Container, P, Member)  is  true   and  P  is the nth
%	(0-based) container membership property.

rdfs_nth0(Index, Container, Member) :-
	rdfs_member(Container, Index, Member).

rdfs_member(Container, N, Member) :-
	(nonvar(Container) ; nonvar(Member)), !,
	rdf_has(Container, P, Member),
	rdfs_container_membership_property(P, N).
rdfs_member(Container, N, Member) :-
	rdfs_container_membership_property(P, N),
	rdf_has(Container, P, Member).
