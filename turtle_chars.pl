/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2009-2013, University of Amsterdam
                              VU University Amsterdam
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

:- module(turtle_unicode,
          [ mkclassify/1,
            run/0
          ]).

/** <module> Generate turtle_chars.c
*/

run :-
    mkclassify('turtle_chars.c', 'static ').

%!  mkclassify(+File)
%
%   Generate the core of xml_unicode.c.

mkclassify(File) :-
    mkclassify(File, '').

mkclassify(File, Decl) :-
    tell(File),
    call_cleanup(forall(list(List, _),
                        mkfunc(List, Decl)),
                 told).

mkfunc(Name, Decl) :-
    format('~wint~n', [Decl]),
    format('wcis_~w(int c)~n', [Name]),
    format('{ '),
    list(Name, List),
    mkswitch(List),
    format('}~n~n').

mkswitch(List) :-
    mkswitch(List, 2).

mkswitch([Low-High], Indent) :-
    !,
    indent(Indent),
    format('return (c >= 0x~|~`0t~16r~4+ && c <= 0x~|~`0t~16r~4+);~n', [Low, High]).
mkswitch([Value], Indent) :-
    !,
    indent(Indent),
    format('return (c == 0x~|~`0t~16r~4+);', [Value]).
mkswitch(List, Indent) :-
    split(List, Low, High),
    end(Low, MaxLow),
    indent(Indent),
    NextIndent is Indent + 2,
    format('if ( c <= 0x~|~`0t~16r~4+ )~n', [MaxLow]),
    indent(Indent),
    format('{ '),
    mkswitch(Low, NextIndent),
    indent(Indent),
    format('} else~n'),
    indent(Indent),
    format('{ '),
    mkswitch(High, NextIndent),
    indent(Indent),
    format('}~n').

end(List, Max) :-
    last(List, Last),
    (   Last = _-Max
    ->  true
    ;   Max = Last
    ).

split(List, Low, High) :-
    length(List, Len),
    Mid is Len//2,
    length(Low, Mid),
    append(Low, High, List).

indent(N) :-
    line_position(current_output, Pos),
    Spaces is N - Pos,
    format('~*c', [Spaces, 32]).



list(pn_chars_base,
     [ 0'A-0'Z,
       0'a-0'z,
       0x00C0-0x00D6,
       0x00D8-0x00F6,
       0x00F8-0x02FF,
       0x0370-0x037D,
       0x037F-0x1FFF,
       0x200C-0x200D,
       0x2070-0x218F,
       0x2C00-0x2FEF,
       0x3001-0xD7FF,
       0xF900-0xFDCF,
       0xFDF0-0xFFFD,
       0x10000-0xEFFFF
     ]).

list(pn_chars_extra,
     [ 0'-,
       0'0-0'9,
       0x00B7,
       0x0300-0x036F,
       0x203F-0x2040
     ]).
