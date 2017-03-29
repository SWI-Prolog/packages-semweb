:- module(test_rdf11,
          [ test_rdf11/0,
            set_graph/1                         % +Name
          ]).
:- include(local_test).
:- use_module(library(semweb/rdf11)).
:- use_module(library(plunit)).
:- use_module(library(debug)).
:- use_module(library(yall)).
:- use_module(library(apply)).

test_rdf11 :-
    rdf_reset_db,
    run_tests([ literal,
                enumerate,
                query_numerical,
                lexical_comparison,
                query_lang,
                query_mixed,
                query_search,
                migrate
              ]).

:- discontiguous
    graph/2.

% Allow for name-space declarations in test  options. The body should be
% properly compiled anyway.

term_expansion((test(Name, Options0) :- Body),
               (test(Name, Options1) :- Body)) :-
    rdf_global_term(Options0, Options1).

                 /*******************************
                 *     LITERAL ASSERT/QUERY     *
                 *******************************/

:- begin_tests(literal, [cleanup(rdf_reset_db)]).

test(integer, L == 42^^xsd:integer) :-
    rdf_assert(integer, p1, 42),
    rdf(integer, p1, L).
test(float, L == 3.2^^xsd:double) :-
    rdf_assert(float, p1, 3.2),
    rdf(float, p1, L).
test(float, [L == 3.0^^xsd:float]) :-
    rdf_assert(float, p2, "3."^^xsd:float),
    rdf(float, p2, L).
test(float, [L == 0.3^^xsd:float]) :-
    rdf_assert(float, p3, ".3"^^xsd:float),
    rdf(float, p3, L).
test(byte, L == 97^^xsd:byte) :-
    rdf_assert(byte, p1, 97^^xsd:byte),
    rdf(byte, p1, L).
test(byte, error(domain_error(T, 200))) :-
    rdf_equal(T, xsd:byte),
    rdf_assert(byte, p2, 200^^xsd:byte).
test(boolean, L == true^^xsd:boolean) :-
    rdf_assert(boolean, p1, true),
    rdf(boolean, p1, L).
test(boolean, L == false^^xsd:boolean) :-
    rdf_assert(boolean, p2, 0^^xsd:boolean),
    rdf(boolean, p2, L).
test(boolean) :-
    rdf_assert(boolean, p3, true),
    rdf(boolean, p3, true).
test(string, L == "hello world"^^xsd:string) :-
    rdf_assert(string, p1, "hello world"),
    rdf(string, p1, L).
test(lang, L == "hello world"@nl) :-
    rdf_assert(string, p2, "hello world"@'NL'),
    rdf(string, p2, L).
test(lang, L == "hello world"@nl) :-
    rdf_assert(string, p3, 'hello world'@'NL'),
    rdf(string, p3, L).
test(date_time, L == date_time(YY,MM,DD,H,M,S,TZ)^^xsd:dateTime) :-
    get_time(Now),                          % time stamp handling
    stamp_date_time(Now, date(YY,MM,DD,H,M,SF,TZM,_TZName,_DST), local),
    S is floor(SF),
    TZ is -TZM,                             % Not very handy!
    rdf_assert(date_time, p1, Now^^xsd:dateTime),
    rdf(date_time, p1, L).
test(date_time, L == date_time(2015,12,13,15,40,0,3600)^^xsd:dateTime) :-
    rdf_assert(date_time, p2, "2015-12-13T15:40:00+01:00"^^xsd:dateTime),
    rdf(date_time, p2, L).
test(date_time, L == date_time(2015,12,13,15,40,0,0)^^xsd:dateTime) :-
    rdf_assert(date_time, p3, "2015-12-13T15:40:00Z"^^xsd:dateTime),
    rdf(date_time, p3, L).
test(untyped, true) :-
    rdf_assert(untyped, p1, "y"^^y),
    \+ rdf(untyped, p1, "x"^^_Type).
test(untyped, Type == y) :-
    rdf_assert(untyped, p1, "y"^^y),
    rdf(untyped, p1, "y"^^Type).
test(int_string, fail) :-
    rdf_assert(int_string, p1, 1),
    rdf(int_string, p1, _^^xsd:string).
test(untyped_int, true) :-
    rdf_assert(untyped, p1, "y"^^y),
    \+ rdf(untyped, p1, "10"^^_Type).
test(untyped_int2, Type == xsd:integer) :-
    rdf_assert(untyped_int2, p1, "010"^^xsd:integer),
    rdf(untyped_int2, p1, 10^^Type).

:- end_tests(literal).

                 /*******************************
                 *      ENUMERATION TESTS       *
                 *******************************/

graph(enumerate_1,
      [ rdf(a, p1, c),
        rdf(c, p2, 42),
        rdf(a, p3, N = [ rdf(N, p3, true),
                         rdf(N, p4, "hello")
                       ])
      ]).

:- begin_tests(enumerate,
               [ setup(assert_graph(enumerate_1, default)),
                 cleanup(rdf_reset_db)
               ]).

% Enumerate by role
test(role, set(S == ['_:genid1', a,c])) :-
    rdf_subject(S).
test(role, set(P == [p1,p2,p3,p4])) :-
    rdf_predicate(P).
test(role, set(O == ['_:genid1', c,
                          42^^xsd:integer,
                          true^^xsd:boolean,
                          "hello"^^xsd:string])) :-
    rdf_object(O).
test(role, set(N == ['_:genid1', a, c,
                          42^^xsd:integer,
                          true^^xsd:boolean,
                          "hello"^^xsd:string])) :-
    rdf_node(N).
test(role, set(N == ['_:genid1', a, c,
                          p1,p2,p3,p4,
                          42^^xsd:integer,
                          true^^xsd:boolean,
                          "hello"^^xsd:string])) :-
    rdf_term(N).
% enumerate by type
test(type, set(IRI == [a,c,p1,p2,p3,p4])) :-
    rdf_iri(IRI).
test(type, set(BNode == ['_:genid1'])) :-
    rdf_bnode(BNode).
test(type, set(Name == [a,c,p1,p2,p3,p4,
                        42^^xsd:integer,
                        true^^xsd:boolean,
                        "hello"^^xsd:string])) :-
    rdf_name(Name).
test(type, set(Literal == [ 42^^xsd:integer,
                            true^^xsd:boolean,
                            "hello"^^xsd:string])) :-
    rdf_literal(Literal).
% deterministic tests
test(enum_det) :- rdf_name(true).
test(enum_det) :- rdf_name(a).
test(enum_det) :- rdf_iri(a).
test(enum_det) :- rdf_subject(a).
test(enum_det) :- rdf_object(c).
test(enum_det) :- rdf_object(true).
test(enum_det) :- rdf_object(42).
test(enum_det) :- rdf_object(42^^xsd:integer).
test(enum_det) :- rdf_object("42"^^xsd:integer).
test(enum_det) :- rdf_object('42'^^xsd:integer).

:- end_tests(enumerate).


                 /*******************************
                 *       NUMERICAL QUERIES      *
                 *******************************/

graph(numerical_1,
      [ rdf(i, p, [-1, 0, 1]),
        rdf(b, p, [-1^^xsd:byte, 0^^xsd:byte, 1^^xsd:byte]),
        rdf(f, p, [-1.0, 0.0, 1.0]),
        rdf(f2, p, [-0.5, 0.5]),
        rdf(s, p, ["-1", "0", "1"]),
        rdf(l, p, ["-1"@en, "0"@en, "1"@en])
      ]).

:- begin_tests(query_numerical,
               [ setup(assert_graph(numerical_1, default)),
                 cleanup(rdf_reset_db)
               ]).

test(eq, set(X == [ 1^^xsd:byte,
                    1^^xsd:integer,
                    1.0^^xsd:double
                  ])) :-
    { X == 1 },
    rdf(_,_,X).
test(eq_pre1, set(X == [ 0.5^^xsd:double
                       ])) :-
    { X == 0.5 },
    rdf(_,_,X).
test(eq_pre2, set(X == [ 0.5^^xsd:double
                       ])) :-
    { X == 0.5^^xsd:double },
    rdf(_,_,X).
test(eq_pre3, set(X == [ 0.5^^xsd:double
                       ])) :-
    X = _^^_,
    { X == 0.5 },
    rdf(_,_,X).
test(eq_pre4, set(X == [ 1^^xsd:byte
                       ])) :-
    { X == 1^^xsd:byte },
    rdf(_,_,X).
test(eq_post1, set(X == [ 0.5^^xsd:double
                        ])) :-
    rdf(_,_,X),
    { X == 0.5 }.
test(eq_post3, set(X == [ 0.5^^xsd:double
                        ])) :-
    X = _^^_,
    rdf(_,_,X),
    { X == 0.5 }.
test(gt, set(X == [ 0.5^^xsd:double,
                    1^^xsd:byte,
                    1^^xsd:integer,
                    1.0^^xsd:double])) :-
    { X >= 0.5 },
    rdf(_,_,X).
test(lt, set(X == [ -0.5^^xsd:double,
                    -1^^xsd:byte,
                    -1^^xsd:integer,
                    -1.0^^xsd:double
                  ])) :-
    { X < 0 },
    rdf(_,_,X).
test(between, set(X == [ -0.5^^xsd:double,      % pure between
                         0.0^^xsd:double,
                         0^^xsd:byte,
                         0^^xsd:integer,
                         0.5^^xsd:double
                       ])) :-
    { X >= -0.5, X =< 0.5 },
    rdf(_,_,X).
test(between, set(X == [ 0.0^^xsd:double,       % between with one constraint
                         0^^xsd:byte,
                         0^^xsd:integer,
                         0.5^^xsd:double
                       ])) :-
    { X > -0.3, X =< 0.5 },
    rdf(_,_,X).
test(between, set(X == [ 0.0^^xsd:double,       % between with two constraints
                         0^^xsd:byte,
                         0^^xsd:integer
                       ])) :-
    { X > -0.3, X < 0.5 },
    rdf(_,_,X).
test(between, set(X == [ 0.0^^xsd:double,       % between with two constraints
                         0^^xsd:byte,
                         0^^xsd:integer
                       ])) :-
    { X > -0.3, Y < 0.5 }, X = Y,
    rdf(_,_,X).
test(between, set(X == [ 0.0^^xsd:double,       % between with two constraints
                         0^^xsd:byte,
                         0^^xsd:integer
                       ])) :-
    { X > -0.3, Y < 0.5 },
    rdf(_,_,X),
    X = Y.

:- end_tests(query_numerical).


                 /*******************************
                 *       LEXICAL COMPARISON     *
                 *******************************/

graph(lexical_1,
      [ rdf(s, nl, ["aap"@nl, "noot"@nl, "lopen"@nl]),
        rdf(s, d,  ["2015-05-23"^^xsd:date, "2016-02-01"^^xsd:date]),
        rdf(s, s,  ["2000", "h2o", "zzz"])
      ]).

:- begin_tests(lexical_comparison,
               [ setup(assert_graph(lexical_1, default)),
                 cleanup(rdf_reset_db)
               ]).

test(date, set(D == [date(2016,2,1)^^xsd:date])) :-
    { D > "2016-01-01"^^xsd:date },
    rdf(_,_,D).
test(date, set(D == [date(2016,2,1)^^xsd:date])) :-
    rdf(_,_,D),
    { D > "2016-01-01"^^xsd:date }.
test(lang, set(D == ["noot"@nl, "lopen"@nl])) :-
    { D > "aap"@nl },
    rdf(_,_,D).
test(lang, set(D == ["noot"@nl, "lopen"@nl])) :-
    rdf(_,_,D),
    { D > "aap"@nl }.
test(lang, set(D == ["h2o"^^xsd:string, "zzz"^^xsd:string])) :-
    { D >= "h2o" },
    rdf(_,_,D).
test(lang, set(D == ["h2o"^^xsd:string, "zzz"^^xsd:string])) :-
    rdf(_,_,D),
    { D >= "h2o" }.

:- end_tests(lexical_comparison).


                 /*******************************
                 *         LANG QUERIES         *
                 *******************************/

graph(lang_1,
      [ rdf(s, nl,  ["Aap"@nl, "Noot"@'NL-nl', "Lopen"@'NL-nl']),
        rdf(s, be,  ['Lopen'@'NL-be']),
        rdf(s, en,  ['Running'@en]),
        rdf(s, str, ["Aap"])
      ]).

:- begin_tests(query_lang,
               [ setup(assert_graph(lang_1, default)),
                 cleanup(rdf_reset_db)
               ]).

test(nl, set(T == [ "Aap" ])) :-
    rdf(_,_,T@nl).
test(nl, set(L == [ "Aap"@nl, "Noot"@'nl-nl', "Lopen"@'nl-nl',
                    "Lopen"@'nl-be'
                  ])) :-
    { lang_matches(L, nl) },
    rdf(_,_,L).
test(pattern, set(L == [ "Running"@en ])) :-
    { lang_matches(L, en) },
    rdf(_,_,L).
test(pattern, set(L == [ "Running"@en ])) :-
    L = _@_,
    { lang_matches(L, en) },
    rdf(_,_,L).
test(pattern, set(L == [ "Running"@en ])) :-
    { lang_matches(L, en) },
    L = _@_,
    rdf(_,_,L).
test(pattern, set(T == [ "Running" ])) :-
    L = T@Lang,
    { lang_matches(Lang, en) },
    rdf(_,_,L).
test(pattern, set(T == [ "Running" ])) :-
    { lang_matches(Lang, en) },
    L = T@Lang,
    rdf(_,_,L).
test(pattern, set(T == [ "Running" ])) :-
    rdf(_,_,T@Lang),
    { lang_matches(Lang, en) }.

:- end_tests(query_lang).


                 /*******************************
                 *          TEXT SEARCH         *
                 *******************************/

graph(search_1,
      [ rdf(s, nl,  ["Aap"@nl, "Noot"@'NL-nl', "Lopen"@'NL-nl']),
        rdf(s, be,  ['Lopen'@'NL-be']),
        rdf(s, en,  ['Running'@en, "realise"@'en-gb']),
        rdf(s, us,  ["realize"@'en-us']),
        rdf(s, str, ["Aap", "h2o"]),
        rdf(s, sen, ["Zwarte zwanen zwemmen in de Zuiderzee"@nl])
      ]).

:- begin_tests(query_search,
               [ setup(assert_graph(search_1, default)),
                 cleanup(rdf_reset_db)
               ]).

test(prefix, set(L==["realise"@'en-gb',"realize"@'en-us'])) :-
    { prefix(L, "rea") },
    rdf(_,_,L).
test(prefix, set(L==["realise"@'en-gb',"realize"@'en-us'])) :-
    rdf(_,_,L),
    { prefix(L, "rea") }.
test(prefix, set(L==["Aap"@nl,"Aap"^^xsd:string])) :-
    { prefix(L, "Aa") },
    rdf(_,_,L).
test(word, set(L==["Zwarte zwanen zwemmen in de Zuiderzee"@nl])) :-
    { word(L, "Zuiderzee") },
    rdf(_,_,L).
test(word, set(L==["Zwarte zwanen zwemmen in de Zuiderzee"@nl])) :-
    { word(L, "zuiderzee") },
    rdf(_,_,L).
test(substring, set(L==["h2o"^^xsd:string])) :-
    { substring(L, "2") },
    rdf(_,_,L).
test(substring, set(L==["h2o"^^xsd:string])) :-
    rdf(_,_,L),
    { substring(L, "2") }.

:- end_tests(query_search).


                 /*******************************
                 *      COMBINE CONDITIONS      *
                 *******************************/

graph(mixed_1,
      [ rdf(s, nl,  ["Aap"@nl, "Noot"@'NL-nl', "Lopen"@'NL-nl']),
        rdf(s, be,  ['Lopen'@'NL-be']),
        rdf(s, en,  ['Running'@en, "realise"@'en-gb']),
        rdf(s, us,  ["realize"@'en-us']),
        rdf(s, str, ["Aap"])
      ]).

:- begin_tests(query_mixed,
               [ setup(assert_graph(mixed_1, default)),
                 cleanup(rdf_reset_db)
               ]).

test(lang_prefix, set(L==["realize"@'en-us'])) :-
    { prefix(L, "rea"),
          lang_matches(L, 'en-us')
        },
    rdf(_,_,L).
test(lang_prefix, set(L==["realize"@'en-us'])) :-
    { lang_matches(L, 'en-us'),
          prefix(L, "rea")
        },
    rdf(_,_,L).
test(lang_prefix, set(L==["realize"@'en-us'])) :-
    { prefix(L, "rea") },
    rdf(_,_,L),
    { lang_matches(L, 'en-us') }.
test(lang_prefix, set(L==["realize"@'en-us'])) :-
    { lang_matches(L, 'en-us') },
    rdf(_,_,L),
    { prefix(L, "rea") }.

:- end_tests(query_mixed).

                 /*******************************
                 *          PLAIN/FULL          *
                 *******************************/

graph(migrate_1,
      [ rdf(s, n, [ 10^^xsd:integer, 11.0^^xsd:double, 12^^xsd:byte ]),
        rdf(s, d, [ "2016-02-01"^^xsd:date ]),
        rdf(s, l, [ "hello"@en, "hallo"@nl ])
      ]).

:- begin_tests(migrate,
               [ setup(assert_graph(migrate_1, default)),
                 cleanup(rdf_reset_db)
               ]).

test(num, set(N == [11.0^^xsd:double, 12^^xsd:byte])) :-
    { N >= 11 },
    rdf(_,_,N).
test(num, set(N == [11.0, 12])) :-
    { N >= 11 },
    rdf(_,_,N^^_T).
test(num, set(N == [11.0, 12])) :-
    rdf(_,_,N^^_T),
    { N >= 11 }.
test(lang, set(T == ["hallo"@nl])) :-
    { lang_matches(T, nl) },
    rdf(_,_,T).
test(lang, set(T == ["hallo"])) :-
    { lang_matches(L, nl) },
    rdf(_,_,T@L).
test(lang, set(T == ["hallo"])) :-
    rdf(_,_,T@L),
    { lang_matches(L, nl) }.

:- end_tests(migrate).


                 /*******************************
                 *       COLLECTIONS            *
                 *******************************/

assert_collections_graph(G) :-
    rdf_assert_list([a,b,c], _, G),
    rdf_assert_list([d,b,e], _, G).

:- begin_tests(collections,
               [ setup(assert_collections_graph(default)),
                 cleanup(rdf_reset_db)
               ]).

test(rdf_length, all(Len =:= [3,3])) :-
    rdf_list(L),
    rdf_length(L, Len).

test(rdf_member, set(X == [a,b,c,d,e])) :-
    rdf_member(X, _).

test(rdf_nextto, set(X-Y == [a-b,b-c,b-e,d-b])) :-
    rdf_nextto(X, Y).

test(rdf_nth0, set(I-X == [0-a,0-d,1-b,2-c,2-e])) :-
    rdf_list(L),
    rdf_nth0(I, L, X).

test(rdf_nth1, set(I-X == [1-a,1-d,2-b,3-c,3-e])) :-
    rdf_list(L),
    rdf_nth1(I, L, X).

test(rdf_last, set(X == [c,e])) :-
    rdf_list(L),
    rdf_last(L, X).

:- end_tests(collections).


                 /*******************************
                 *           TEST DATA          *
                 *******************************/

:- rdf_meta
    assert_graph(t, r).

%!  set_graph(+Name) is det.
%
%   Set the indicated graph.

set_graph(Name) :-
    rdf_reset_db,
    rdf_default_graph(Graph),
    assert_graph(Name, Graph).

%!  assert_graph(+Name, +Graph)
%
%   Assert data from graph/2 into the RDF graph Graph.

assert_graph(Name, Graph) :-
    atom(Name),
    !,
    (   graph(Name, Data0)
    ->  rdf_global_term(Data0, Data),
        assert_graph(Data, Graph)
    ;   existence_error(graph, Name)
    ).
assert_graph(Data, Graph) :-
    term_variables(Data, BNodes),
    maplist(rdf_create_bnode, BNodes),
    maplist({Graph}/[Triple]>>assert_triple(Triple,Graph), Data).

assert_triple(rdf(S,P,BNode=Triples), G) :-
    !,
    assertion(rdf_is_bnode(BNode)),
    rdf_assert(S,P,BNode),
    maplist({G}/[Triple]>>assert_triple(Triple, G), Triples).
assert_triple(rdf(S,P,LO), G) :-
    is_list(LO),
    !,
    maplist({S,P,G}/[O]>>assert_triple(rdf(S,P,O), G), LO).
assert_triple(rdf(S,P,O), G) :-
    rdf_assert(S,P,O,G).
