/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2017, VU University Amsterdam
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

#include "xsd.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static xsd_type xsd_types[] =
{
  { URL_xsd "byte",		  0, XSD_INTEGER, -128,        127 },
  { URL_xsd "double",		  0, XSD_DOUBLE,   0,	       0 },
  { URL_xsd "decimal",		  0, XSD_DECIMAL,  0,	       0 },
  { URL_xsd "int",		  0, XSD_INTEGER, -2147483648, 2147483647 },
  { URL_xsd "integer",		  0, XSD_INTEGER,  0,	       0 },
  { URL_xsd "long",		  0, XSD_INTEGER,  0,	       0 },
  { URL_xsd "negativeInteger",	  0, XSD_INTEGER,  0, -1 },
  { URL_xsd "nonPositiveInteger", 0, XSD_INTEGER,  0,	       0 },
  { URL_xsd "positiveInteger",	  0, XSD_INTEGER,  1,	       0 },
  { URL_xsd "short",		  0, XSD_INTEGER, -32768,      32767 },
  { URL_xsd "unsignedByte",	  0, XSD_INTEGER,  0,	       255 },
  { URL_xsd "unsignedInt",	  0, XSD_INTEGER,  0,	       4294967295 },
  { URL_xsd "unsignedLong",	  0, XSD_INTEGER,  0,	       0 },
  { URL_xsd "unsignedShort",	  0, XSD_INTEGER,  0,	       65535 },
  { NULL }
};


void
xsd_init(void)
{ xsd_type *t;
  static int done = FALSE;

  if ( done )
    return;

  for(t=xsd_types; t->url; t++)
    t->url_atom = PL_new_atom(t->url);

  done = TRUE;
}


xsd_primary
is_numeric_type(atom_t type)
{ const xsd_type *t;

  xsd_init();
  for(t=xsd_types; t->url_atom; t++)
  { if ( t->url_atom == type )
      return t->primary;
  }

  return XSD_NONNUMERIC;
}

/* BUG: goes wrong if locale is switched at runtime.  But, doing
   this dynamically is a more than 100% overhead on xsd_number_string/2
   and changing locale after process initialization is uncommon and
   a bad idea anyway.
*/

static int
decimal_dot(void)
{ static int ddot = '\0';

  if ( ddot )
    return ddot;

  char buf[10];
  sprintf(buf, "%f", 1.0);
  ddot = buf[1];

  return ddot;
}

static double
strtod_C(const char *in, char **eptr)
{ int dot;

  if ( (dot=decimal_dot()) != '.' )
  { char fast[64];
    size_t len = strlen(in);
    char *fs = len < sizeof(fast) ? fast : malloc(len+1);
    char *o;
    double v;
    char *eptr2;
    const char *s;

    if ( !fs )
      return strtod("NaN", &eptr2);
    for(s=in,o=fs; *s; s++,o++)
    { if ( (*o=*s) == '.' )
	*o = dot;
    }
    *o = '\0';
    v = strtod(fs, &eptr2);
    *eptr = (char*)in+(fs-eptr2);
    if ( fs != fast )
      free(fs);

    return v;
  } else
  { return strtod(in, eptr);
  }
}



int
xsd_compare_numeric(xsd_primary type1, const unsigned char *s1,
		    xsd_primary type2, const unsigned char *s2)
{ if ( type1 == XSD_INTEGER && type2 == XSD_INTEGER )
  { size_t l1, l2;
    int mul = 1;

    if ( *s1 == '-' && *s2 != '-' ) return -1;
    if ( *s2 == '-' && *s1 != '-' ) return 1;
    if ( *s1 == '-' && *s2 == '-' )
      s1++, s2++, mul = -1;
    if ( *s1 == '+' ) s1++;
    if ( *s2 == '+' ) s2++;

    while(*s1 == '0') s1++;
    while(*s2 == '0') s2++;
    l1 = strlen((const char*)s1);
    l2 = strlen((const char*)s2);
    if ( l1 != l2 )
      return (l1 < l2 ? -1 : 1) * mul;

    return strcmp((const char*)s1, (const char*)s2) * mul;
  } else
  { char *e1, *e2;
    double v1 = strtod_C((const char*)s1, &e1);
    double v2 = strtod_C((const char*)s2, &e2);

    if ( !*e1 && !*e2 )
    { return v1 < v2 ? -1 :
	     v1 > v2 ?  1 : 0;
    }

    return strcmp((const char*)s1, (const char*)s2);
  }
}


int
cmp_xsd_info(xsd_primary type1, atom_info *v1,
	     xsd_primary type2, atom_t v2)
{ text t2;

  if ( fill_atom_info(v1) &&
       v1->text.a &&
       fetch_atom_text(v2, &t2) &&
       t2.a )
  { return xsd_compare_numeric(type1, v1->text.a, type2, t2.a);
  } else
  { return v1->handle < v2 ? -1 : 1;	/* == already covered */
  }
}
