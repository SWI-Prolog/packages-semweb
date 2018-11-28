:- module(lit,
          [ lit/0
          ]).
:- prolog_load_context(directory, D),
   asserta(user:file_search_path(library, D)),
   atom_concat(D, '/..', DD),
   asserta(user:file_search_path(library, DD)).
:- use_module(library('semweb/rdf_db')).

data(string, '').
data(string, 'This is a nice string').

data(int, 0).
data(int, -67).
data(int, 327848).

data(float, 0.0).
data(float, 48.25).

data(term, [let, us, test, a, list]).
data(term, [let, us, test, another, list]).

create :-
    (   data(Type, Value),
        rdf_assert(subject, Type, literal(Value)),
        fail
    ;   true
    ).

lookup :-
    findall(T-V, (rdf(subject, T, X), X = literal(V)), Pairs),
    findall(T-V, data(T, V), Data),
    Data == Pairs.


                 /*******************************
                 *              MAIN            *
                 *******************************/

lit :-
    rdf_reset_db,
    create,
    lookup.


