:- module(bench,
          [ bench/0,
            bench_first/1,
            create/1,                   % +Triples
            remove_p/1,                 % +Fraction
            stress/2                    % SizeM, Threads
          ]).
:- include(local_test).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(apply_macros)).
:- use_module(library(statistics)).
:- use_module(library(random)).

:- if(\+current_predicate(rdf_resource/1)).
rdf_resource(S) :-
    rdf_subject(S).
rdf_resource(S) :-
    rdf_current_predicate(S).
:- endif.

:- initialization set_random(seed(111)).

bench :-
    time(create(100000)),
    bench_first(100).

%!  bench_first
%
%   Query the first triple of all resources

bench_first(N) :-
    findall(Q, rdf_resource(Q), Resources),
    length(Resources, Len),
    format('~D resources~n', [Len]),
    time(forall(between(1, N, _),
                forall(member(Q,Resources),
                       first_triple(Q)))).

first_triple(Q) :-
    rdf(Q,_,_),
    !.
first_triple(_).

%!  create(N)
%
%   Create an RDF DB with N triples

create(N) :-
    flag(offset, Off, Off+N),
    create(N,Off).

create(N,Off) :-
    forall(between(1, N, I),
           add_random(N, I, Off)).

add_random(N, I, Off) :-
    random_triple(N,I,Off, S,P,O),
    rdf_assert(S, P, O).

random_triple(N,I,Off, S,P,O) :-
    random_subject(N, I, Off, S),
    random_predicate(N, I, Off, P),
    random_object(N, I, Off, O).

random_subject(N,_I,Off,S) :-
    I is random(N//10)+Off,
    atom_concat(s,I,S).

random_predicate(_N,_I,_Off,S) :-
    I is random(10),
    atom_concat(p,I,S).

random_object(N,I,Off,O) :-
    (   maybe(0.3),fail
    ->  R is random(N//30),
        atom_concat(l,R,L),
        O = literal(L)
    ;   random_subject(N,I,Off,O)
    ).

%!  remove_p(+P)
%
%   Remove a fraction of the DB.  E.g., remove_p(0.1) removes
%   10% of the DB.

remove_p(Prob) :-
    (   rdf(S,P,O),
        maybe(Prob),
        rdf_retractall(S,P,O),
        fail
    ;   true
    ).


%!  stress(SizeM, FillerThreads)
%
%   Start threads that fill, remove and GC the DB.

stress(SizeM, Fillers) :-
    MaxCount = round(SizeM*1000000),
    forall(between(1, Fillers, FI),
           (   atom_concat(filler_, FI, Alias),
               thread_create(filler(MaxCount), _, [alias(Alias)])
           )),
    thread_create(emptier, _, [alias(emptier)]).

filler(MaxCount) :-
    rdf_statistics(triples(Count)),
    (   Count > MaxCount
    ->  sleep(1)
    ;   Bunch = 100000,
        Offset is random(MaxCount-Bunch),
        time(create(Bunch,Offset))
    ),
    filler(MaxCount).

emptier :-
    remove_p(0.1),
    emptier.

