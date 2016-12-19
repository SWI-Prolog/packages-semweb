:- module(rdf_test,
          [ run/0,                      % Run all tests
            run/1,                      % +Test
            (+)/1,                      % Assert
            (-)/1,                      % Retract
            v/1,                        % Visible
            s/2,                        % +Id, ?Subject
            p/2,                        % +Id, ?Predicate
            o/2,                        % +Id, ?Object
            u/1,                        % InVisible
            l/0,                        % List
            r/0,                        % reset
            {}/1,                       % transaction
            (@@)/2,                     % Action @ Context (Synchronous)
            (@@)/1,                     % Action in snapshot
            j/0,                        % Join helper threads
            j/1,                        % Join a specific helper
            jf/1,                       % Join a specific helper, failing
            snap/1,                     % -Snapshot

            op(200, xfx, @@),
            op(200, xf,  @@),
            op(200, xfy, <=)
          ]).
:- include(local_test).

:- use_module(rdf_db).
:- use_module(library(aggregate)).

/** <module> RDF test language
*/

:- meta_predicate
    true(0),
    false(0),
    {}(0),
    @@(:,?),
    @@(:).

:- thread_local
    triple/2.

%!  + Pattern
%
%   Assert a triple, optionally giving it a name.

+ Name^rdf(S,P,O) :-
    !,
    mk_spo(S,P,O),
    rdf_assert(S,P,O),
    (   var(Name)
    ->  Name = rdf(S,P,O)
    ;   assertz(triple(Name, rdf(S,P,O)))
    ).
+ rdf(S,P,O) :-
    !,
    mk_spo(S,P,O),
    rdf_assert(S,P,O).
+ Sub<=Super :-
    mk(p,Sub),
    mk(p,Super),
    rdf_assert(Sub, rdfs:subPropertyOf, Super).

mk_spo(S,P,O) :-
    mk(s, S),
    mk(p, P),
    mk(o, O).

mk(_, R) :- atom(R), !.
mk(Prefix, R) :-
    gensym(Prefix, R).

%!  - Pattern
%
%   Retract a triple, normally referenced by name.

- Name^rdf(S,P,O) :-
    !,
    rdf_retractall(S,P,O),
    (   var(Name)
    ->  Name = rdf(S,P,O)
    ;   assertz(triple(Name, rdf(S,P,O)))
    ).
- rdf(S,P,O) :-
    !,
    rdf_retractall(S,P,O).
- rdf(S,P,O) :-
    !,
    rdf_retractall(S,P,O).
- Name :-
    ground(Name),
    triple(Name, Triple),
    -(Triple).

%!  v(+Id)
%
%   True if triple Id is visible.

v(+rdf(S,P,O)) :-
    !,
    v(rdf_has(S,P,O)).
v(rdf(S,P,O)) :-
    !,
    true((rdf(S,P,O))),
    true((rdf(S,P,O2), O == O2)),
    true((rdf(S,P2,O), P == P2)),
    true((rdf(S,P2,O2), P == P2, O == O2)),
    true((rdf(S2,P,O), S2 == S)),
    true((rdf(S2,P,O2), S2 == S, O == O2)),
    true((rdf(S2,P2,O), S2 == S, P == P2)),
    true((rdf(S2,P2,O2), S2 == S, P == P2, O == O2)).
v(rdf_has(S,P,O)) :-
    !,
    true((rdf_has(S,P,O))),
    true((rdf_has(S,P,O2), O == O2)),
    true((rdf_has(S2,P,O), S2 == S)),
    true((rdf_has(S2,P,O2), S2 == S, O == O2)).
v(Name) :-
    ground(Name),
    triple(Name, Triple),
    v(Triple).

%!  u(+Id)
%
%   True if triple Id is not visible.

u(rdf(S,P,O)) :-
    !,
    false((rdf(S,P,O))).
u(+rdf(S,P,O)) :-
    !,
    false((rdf_has(S,P,O))).
u(Name) :-
    ground(Name),
    triple(Name, Triple),
    u(Triple).

%!  s(Id, Subject) is semidet.
%!  p(Id, Predicate) is semidet.
%!  o(Id, Object) is semidet.

s(rdf(S,_,_), T) :-
    !,
    S = T.
s(Name, T) :-
    ground(Name),
    triple(Name, Triple),
    s(Triple, T).
p(rdf(_,P,_), T) :-
    !,
    P = T.
p(Name, T) :-
    ground(Name),
    triple(Name, Triple),
    p(Triple, T).
o(rdf(_,_,O), T) :-
    !,
    O = T.
o(Name, T) :-
    ground(Name),
    triple(Name, Triple),
    o(Triple, T).



true(G) :-
    G,
    !.
true(G) :-
    print_message(error, false(G)),
    backtrace(5),
    throw(test_failed).

false(G) :-
    G,
    !,
    print_message(error, true(G)),
    backtrace(5),
    throw(test_failed).
false(_).

%!  {G}
%
%   Run G in an RDF transaction.

{}(G) :-
    rdf_transaction(G).

%!  snap(-Snapshot) is det.
%
%   Create a snapshot.

snap(X) :-
    rdf_snapshot(X).

%!  @@(:Goal, +Context)
%
%   Run Goal (as once/1) in Context. Context is either a snapshot or
%   a seperate thread.  If  Context  is   a  thread,  wait  for  its
%   completion (synchronous execution). The construct
%
%           {G} @@ {T}
%
%   runs the helper thread as a transaction.

:- dynamic
    helper/1.

(M:{}(G)) @@ Snapshot :-
    nonvar(Snapshot),
    rdf_current_snapshot(Snapshot),
    !,
    rdf_transaction(M:G, _Id, [snapshot(Snapshot)]).
(M:{}(G)) @@ T :-
    (   var(T)
    ->  thread_create(helper, T, []),
        assert(helper(T)),
        Helper = T
    ;   T = {Helper}
    ->  (   var(Helper)
        ->  thread_create(rdf_transaction(helper), Helper, []),
            assert(helper(Helper))
        ;   true
        )
    ;   Helper = T
    ),
    thread_self(Me),
    thread_send_message(Helper, run(M:G, Me)),
    thread_get_message(Reply),
    (   Reply = true(X)
    ->  X = M:G
    ;   Reply = exception(E)
    ->  throw(E)
    ).

%!  @@(:Goal)
%
%   Run Goal in a snapshot.

((M:{}(G)) @@) :-
    rdf_transaction(M:G, _Id, [snapshot(true)]).

%!  j
%
%   Join all helper threads.

j :-
    forall(retract(helper(Id)),
           (   thread_send_message(Id, done),
               thread_join(Id, true)
           )).

%!  j(+Id)
%
%   Join the specific helper thread, terminating its transaction
%   with success.

j(Id) :-
    retract(helper(Id)),
    !,
    thread_send_message(Id, done),
    thread_join(Id, true).

jf(Id) :-
    retract(helper(Id)),
    !,
    thread_send_message(Id, fail),
    thread_join(Id, false).

helper :-
    thread_get_message(M),
    (   M = run(G, Sender)
    ->  run(G,Result),
        thread_send_message(Sender, Result),
        helper
    ;   M == done
    ).

run(G, Result) :-
    catch(G, E, true),
    !,
    (   var(E)
    ->  Result = true(G)
    ;   Result = exception(E)
    ).
run(_, false).


%!  r
%
%   Reset the RDF database, helper threads, etc.

r :-
    j,
    retractall(triple(_,_)),
    rdf_reset_db.

%!  l
%
%   List content of RDF database.

l :-
    forall(rdf(S,P,O),
           format('<~q, ~q, ~q>~n', [S,P,O])).


db(RDF) :-
    findall(rdf(S,P,O), rdf(S,P,O), RDF0),
    sort(RDF0, RDF).

                 /*******************************
                 *             TESTS            *
                 *******************************/

:- op(1000, fx, test).

:- discontiguous (test)/1.

term_expansion((test Head :- Body),
               [ test(Head),
                 (Head :- Body)
               ]).

test t1 :-                              % asserted triple in failed
    (  { + a^_,                     % transaction disappears
         fail
           }
    ;  true
    ),
    u(a).
test t2 :-                              % asserted triple in transaction
    { + a^_,                        % is visible inside and outside
      v(a)
        },
    v(a).
test t3 :-
    { + a^_,
          { v(a),
            + b^_,
            v(b)
          },
          v(b)
        },
    v(a).
test t4 :-
    + a^_,
    { v(a)
        }.
test t5 :-
    + a^_,
    { - a,
          u(a)
        },
    u(a).
test t6 :-
    + a^_,
    { - a,
          u(a)
        },
    u(a).
test t7 :-
    + a^_,
    (   { - a,
              u(a),
              fail
            }
    ;   true
    ),
    v(a).
                                                % property handling tests
test p1 :-
    + rdf(s,p,_),
    + B^rdf(s,p,_),
    rdf(s,p,O),
    - B,
    o(B, O).
test p2 :-
    + rdf(s,p,_),
    + B^rdf(s,p,_),
    rdf(s,p,O),
    {- B} @@ H,
    {u(B)} @@ H,
    u(B),
    o(B, O).
test p3 :-
    + B^rdf(s,p,_),
    {-B}@@_,
    u(B).
                                                % snapshot tests
test s1 :-
    + a^_,
    snap(S),
    + b^_,
    { u(b) }@@S.

test s2 :-
    + a^_,
    snap(S),
    { + b^_
    }@@S,
    u(b).

test s3 :-
    + a^_,
    snap(S),
    { - a
    }@@S,
    v(a).

test s4 :-
    + a^_,
    { - a
    }@@,
    v(a).

test s5 :-                              % snap inside a transaction
    { + a^_,
          snap(S),
          + b^_,
          { u(b) }@@S
        }.

/* subProperty tests */

test sp1 :-
    + rdf(S1,P1,O1),
    + P1<=P2,
    v(+rdf(S1,P2,O1)).

test sp2 :-
    + rdf(S1,P1,O1),
    { + P1<=P2 } @@ _,
    v(+rdf(S1,P2,O1)).

test sp3 :-
    + rdf(S1,P1,O1),
    { + P1<=P2 } @@ {T},
    u(+rdf(S1,P2,O1)),
    j(T),
    v(+rdf(S1,P2,O1)).

test sp3b :-
    + rdf(S1,P1,O1),
    { { + P1<=P2 }
    } @@ {T},
    u(+rdf(S1,P2,O1)),
    j(T),
    v(+rdf(S1,P2,O1)).

test sp4 :-
    + rdf(S1,P1,O1),
    { + P1<=P2 } @@ {T},
    u(+rdf(S1,P2,O1)),
    jf(T),
    u(+rdf(S1,P2,O1)).

test sp5 :-                             % join two non-empty clouds
    + rdf(S1,P1,O1),
    + P1 <= SP1,
    v(+rdf(S1,SP1,O1)),
    + rdf(S2,P2,O2),
    + P2 <= SP2,
    v(+rdf(S2,SP2,O2)),
    + SP1 <= Root,
    + SP2 <= Root,
    v(+rdf(S1,Root,O1)),
    v(+rdf(S2,Root,O2)).

/* Logical updates */

r2(R, R2) :-
    atom_concat(R, '^', R2).

test lu1 :-
    + _,
    findall(x, (rdf(S,P,O), r2(O, O2), rdf_assert(S,P,O2)), [x]),
    findall(x, (rdf(S,P,O), r2(S, S2), rdf_assert(S2,P,O)), [x,x]).

test lu2 :-
    + rdf(S1,P1,_),
    + rdf(S2,P2,_),
    + P1<=Root,
    findall(x, ( rdf_has(_,Root,_),
                 + P2<=Root
               ), [x]),
    findall(S, rdf_has(S, Root, _), [S1,S2]).


/* duplicate handling */

test dup1 :-
    + X,
    + X,
    findall(x, rdf(_,_,_), [x]).

test dup2 :-
    + X,
    + X,
    - X,
    findall(x, rdf(_,_,_), []).

test dup3 :-
    + X,
    \+ { + X, fail },
    findall(x, rdf(_,_,_), [x]).


        /*******************************
        *           TEST DRIVER         *
        *******************************/

:- dynamic
    passed/1,
    failed/1.

%!  run
%
%   Run all tests

run :-
    retractall(passed(_)),
    retractall(failed(_)),
    forall(test(Head),
           run(Head)),
    aggregate_all(count, passed(_), Passed),
    aggregate_all(count, failed(_), Failed),
    (   Failed =:= 0
    ->  format('~NAll ~D tests passed~n', [Passed])
    ;   format('~N~D tests passed; ~D failed~n', [Passed, Failed]),
        fail
    ).

%!  run(+Test)
%
%   Run one individual test.

run(Head) :-
    r,
    catch(Head, E, true),
    !,
    j,
    (   var(E)
    ->  assert(passed(Head)),
        write(user_error, '.')
    ;   assert(failed(Head)),
        (   E == test_failed
        ->  print_message(error, test_failed(Head))
        ;   print_message(error, test_failed(Head, E))
        )
    ).
run(Head) :-
    j,
    assert(failed(Head)),
    print_message(error, test_failed(Head)).


                 /*******************************
                 *            MESSAGES          *
                 *******************************/

prolog:message(test_failed) -->
    [ 'Test failed' ].
prolog:message(test_failed(Head)) -->
    [ 'Test failed: ~q'-[Head] ].
prolog:message(test_failed(Head, Error)) -->
    [ 'Test failed: ~q: '-[Head] ],
    '$messages':translate_message(Error).
prolog:message(false(Goal)) -->
    [ 'Unexpected failure: ~q'-[Goal] ].
prolog:message(true(Goal)) -->
    [ 'Unexpected success: ~q'-[Goal] ].
