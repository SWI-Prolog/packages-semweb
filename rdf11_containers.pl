/*  Part of SWI-Prolog

    Author:        Jan Wielemaker and Wouter Beek
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2016, VU University Amsterdam
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

:- module(rdf11_containers,
	  [ rdf_alt/1,				  % ?Alt
	    rdf_alt/3,				  % ?Alt, -Default, -Others
	    rdf_assert_alt/3,			  % +Default, +Others, ?Alt
	    rdf_assert_alt/4,			  % +Default, +Others, ?Alt, +G
	    rdf_assert_bag/2,			  % +Set, ?Bag
	    rdf_assert_bag/3,			  % +Set, ?Bag, +G
	    rdf_assert_seq/2,			  % +List, ?Seq
	    rdf_assert_seq/3,			  % +List, ?Seq, +G
	    rdf_bag/1,				  % ?Bag
	    rdf_bag/2,				  % ?Bag, -Set
	    rdf_seq/1,				  % ?Seq
	    rdf_seq/2,				  % ?Seq, -List
	    rdfs_container/1,			  % ?Container
	    rdfs_container/2,			  % ?Container, -List
	    rdfs_container_membership_property/1, % ?Property
	    rdfs_container_membership_property/2, % ?Property, ?Number
	    rdfs_member/2,			  % ?Elem, ?Container
	    rdfs_nth1/3				  % ?N, ?Container, ?Elem
	  ]).
:- use_module(library(apply)).
:- use_module(library(pairs)).
:- use_module(library(error)).
:- use_module(library(solution_sequences)).
:- use_module(rdf11).
:- use_module(rdfs, [rdfs_individual_of/2]).

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
@version 2016/01, 2016/10
*/

:- rdf_meta
	rdf_alt(r),
	rdf_alt(r, -, -),
	rdf_assert_alt(r, t, r),
	rdf_assert_alt(r, t, r, r),
	rdf_assert_bag(t, r),
	rdf_assert_bag(t, r, r),
	rdf_assert_seq(t, r),
	rdf_assert_seq(t, r, r),
	rdf_bag(r),
	rdf_bag(r, -),
	rdf_seq(r),
	rdf_seq(r, -),
	rdfs_assert_container(t, r),
	rdfs_assert_container(t, r, r),
	rdfs_assert_container(r, t, r, r),
	rdfs_container(r),
	rdfs_container(r, -),
	rdfs_container_membership_property(r),
	rdfs_container_membership_property(r,?),
	rdfs_member(r, -).


%%	rdf_alt(+Alt) is semidet.
%%	rdf_alt(-Alt) is nondet.
%%	rdf_alt(+Alt, -Default, -Others) is multi.
%%	rdf_alt(-Alt, -Default, -Others) is nondet.
%
%	True when Alt is an instance of `rdf:Alt` with first member
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

rdf_alt(Alt) :-
	rdfs_individual_of(Alt, rdf:'Alt').

rdf_alt(Alt, Default, Others) :-
	rdf_alt(Alt),
	rdfs_container_(Alt, [Default|Others]).

%%	rdf_assert_alt(+Default, +Others:list, ?Alt) is det.
%%	rdf_assert_alt(+Default, +Others:list, ?Alt, +Graph) is det.
%
%	Create an rdf:Alt with the given Default and Others. Default and
%	the  members  of  Others  must  be    valid   object  terms  for
%	rdf_assert/3.
%
%	Conventionally, the Default value is asserted with container
%	membership relation rdf:_1.

rdf_assert_alt(H, T, Alt) :-
	rdf_default_graph(G),
	rdf_assert_alt(Alt, H, T, G).

rdf_assert_alt(H, T, Alt, G) :-
	rdfs_assert_container(rdf:'Alt', [H|T], Alt, G).


%%	rdf_bag(+Bag) is semidet.
%%	rdf_bag(-Bag) is nondet.
%%	rdf_bag(+Bag, -Set:list) is multi.
%%	rdf_bag(-Bag, -Set:list) is nondet.
%
%	True when Bag is an rdf:Bag and   Set  is the set values related
%	through container membership properties to Bag.
%
%	Notice that this construct adds no machine-processable semantics
%	but is conventionally used to indicate   to  a human reader that
%	the numerical ordering of the container membership properties of
%	Container is intended to not be significant.

rdf_bag(Bag) :-
	rdfs_individual_of(Bag, rdf:'Bag').

rdf_bag(Bag, Set) :-
	rdf_bag(Bag),
	rdfs_container_(Bag, Set).


%%	rdf_assert_bag(+Set:list, ?Bag) is det.
%%	rdf_assert_bag(+Set:list, ?Bag, +Graph) is det.
%
%	Create an rdf:Bag from the given set   of values. The members of
%	Set must be valid object terms for rdf_assert/3.

rdf_assert_bag(L, Bag) :-
	rdf_default_graph(G),
	rdf_assert_bag(L, Bag, G).

rdf_assert_bag(L, Bag, G) :-
	rdfs_assert_container(rdf:'Bag', L, Bag, G).


%%	rdf_seq(+Seq) is semidet.
%%	rdf_seq(-Seq) is nondet.
%%	rdf_seq(+Seq, -List:list) is multi.
%%	rdf_seq(-Seq, -List:list) is nondet.
%
%	True when Seq is an instance of rdf:Seq   and  List is a list of
%	associated values, ordered according to the container membership
%	property used.
%
%	Notice that this construct adds no machine-processable semantics
%	but is conventionally used to indicate   to  a human reader that
%	the numerical ordering of the container membership properties of
%	Container is intended to be significant.

rdf_seq(Seq) :-
	rdfs_individual_of(Seq, rdf:'Seq').

rdf_seq(Seq, L) :-
	rdf_seq(Seq),
	rdfs_container_(Seq, L).


%%	rdf_assert_seq(+List, ?Seq) is det.
%%	rdf_assert_seq(+List, ?Seq, +Graph) is det.

rdf_assert_seq(L, Seq) :-
	rdf_default_graph(G),
	rdf_assert_seq(L, Seq, G).
rdf_assert_seq(L, Seq, G) :-
	rdfs_assert_container(rdf:'Seq', L, Seq, G).


%%	rdfs_assert_container(+Class, +Elems, ?Container, +Graph)

rdfs_assert_container(Class, Elems, Container, G) :-
	must_be(list, Elems),
	rdf_transaction(rdfs_assert_container_(Class, Elems, Container, G)).

rdfs_assert_container_(Class, Elems, Container, G) :-
	(var(Container) -> rdf_create_bnode(Container) ; true),
	rdf_assert(Container, rdf:type, Class, G),
	rdfs_assert_members(Elems, 1, Container, G).

rdfs_assert_members([], _, _, _).
rdfs_assert_members([H|T], N1, Resource, G) :- !,
	rdf_equal(rdf:'_', Prefix),
	atom_concat(Prefix, N1, P),
	rdf_assert(Resource, P, H, G),
	N2 is N1 + 1,
	rdfs_assert_members(T, N2, Resource, G).


%%	rdfs_container(+Container) is semidet.
%%	rdfs_container(-Container) is nondet.
%%	rdfs_container(+Container, -List) is multi.
%%	rdfs_container(-Container, -List) is nondet.
%
%	True when List is the  list   of  objects  attached to Container
%	using a container membership property  (rdf:_0, rdf:_1, ...). If
%	multiple objects are connected to the   Container using the same
%	membership  property,  this   predicate    selects   one   value
%	non-deterministically.

rdfs_container(Container) :-
	distinct(Container, rdfs_container_(Container)).

rdfs_container_(Alt) :-
	rdf_alt(Alt).
rdfs_container_(Bag) :-
	rdf_bag(Bag).
rdfs_container_(Seq) :-
	rdf_seq(Seq).

rdfs_container(Container, List) :-
	rdfs_container(Container),
	rdfs_container_(Container, List).

rdfs_container_(Container, List) :-
	findall(N-Elem, rdfs_member_(Container, N, Elem), Pairs),
	keysort(Pairs, Sorted),
	group_pairs_by_key(Sorted, GroupedPairs),
	pairs_values(GroupedPairs, Groups),
	maplist(member, List, Groups).


%%	rdfs_container_membership_property(?Property) is nondet.
%
%	True when Property is a   container membership property (rdf:_1,
%	rdf:_2, ...).

rdfs_container_membership_property(P) :-
	rdfs_container_membership_property(P, _).


%%	rdfs_container_membership_property(?Property, ?Number:nonneg) is nondet.
%
%	True when Property is the Nth container membership property.
%
%	Success of this goal does not imply that Property is present
%	in the database.

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


%!	rdfs_member(?Elem, ?Container) is nondet.
%
%	True if rdf(Container, P, Elem) is true   and P is a container
%	membership property.

rdfs_member(Elem, Container) :-
	rdfs_member_(Container, _, Elem).


%!	rdfs_nth1(?N, ?Container, ?Elem) is nondet.
%
%	True if rdf(Container, P, Elem)  is  true   and  P  is the N-th
%	(0-based) container membership property.

rdfs_nth1(N, Container, Elem) :-
	rdfs_member_(Container, N, Elem).


%!	rdfs_member_(?Container, ?N, ?Elem) is nondet.
%
%	What is the most efficient way to enumerate
%	`rdfs_member_(-,-)`?
%
%	1. If we enumerate all container membership properties (=
%	the current implementation) then it takes N steps before we
%	get to triple `〈Container, rdf:_N, Elem〉`, for arbitrary
%	N.
%
%	2. The alternative is to enumerate over all triples and check
%	whether the predicate term is a container membership property.
%
%	3. The choice between (1) and (2) depends on whether the
%	number of currently loaded triples in larger/smaller than the
%	largest number that appears in a container membership
%	property.  This means enumerating over all predicate terms
%	using rdf_predicate/1.

rdfs_member_(Container, N, Elem) :-
	(nonvar(Container) ; nonvar(Elem)), !,
	rdf_has(Container, P, Elem),
	rdfs_container_membership_property(P, N).
rdfs_member_(Container, N, Elem) :-
	rdfs_container_membership_property(P, N),
	rdf_has(Container, P, _),
	rdfs_member_(Container, N, Elem).
