/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2011-2013, VU University Amsterdam
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

#ifndef RDF_MEMORY_H_INCLUDED
#define RDF_MEMORY_H_INCLUDED

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Stuff for lock-free primitives

  * MSB(unsigned int i)
  Computes the most-significant bit+1, which often translates to
  a single machine operation.

     MSB(0) = 0
     MSB(1) = 1
     MSB(2) = 2
     ...

     blocks[0] --> [0]
     blocks[1] --> [1]
     blocks[2] --> [2,3]
     ...

     size of array at i is i=0 ? 1 : 1<<(i-1)

  * MemoryBarrier()
  Realises a (full) memory barrier.  This means that memory operations
  before the barrier are all executed before those after the barrier.

  * PREFETCH_FOR_WRITE(p)
  * PREFETCH_FOR_READ(p)
  Cache-prefetch instructions
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define BLOCKLEN(i) ((i) ? 1<<(i-1) : 1)

#ifdef _MSC_VER				/* Windows MSVC version */

static inline int
MSB(unsigned int i)
{ unsigned long mask = i;
  unsigned long index;

  assert(0);				/* TBD */

  _BitScanReverse(&index, mask);	/* 0 if mask is 0 */
  return index;
}

#elif defined(__GNUC__)			/* GCC version */

#define MSB(i)			((i) ? (32 - __builtin_clz(i)) : 0)
#define MEMORY_BARRIER()	__sync_synchronize()
#define PREFETCH_FOR_WRITE(p)	__builtin_prefetch(p, 1, 0)
#define PREFETCH_FOR_READ(p)	__builtin_prefetch(p, 0, 0)
#define ATOMIC_ADD(ptr, v)	__sync_add_and_fetch(ptr, v)
#define ATOMIC_SUB(ptr, v)	__sync_sub_and_fetch(ptr, v)
#define ATOMIC_INC(ptr)		ATOMIC_ADD(ptr, 1) /* ++(*ptr) */
#define ATOMIC_DEC(ptr)		ATOMIC_SUB(ptr, 1) /* --(*ptr) */

#endif /*_MSC_VER|__GNUC__*/

#ifndef MSB
static inline int
MSB(unsigned int i)
{ int j = 0;

  if (i >= 0x10000) {i >>= 16; j += 16;}
  if (i >=   0x100) {i >>=  8; j +=  8;}
  if (i >=    0x10) {i >>=  4; j +=  4;}
  if (i >=     0x4) {i >>=  2; j +=  2;}
  if (i >=     0x2) j++;
  if (i >=     0x1) j++;

  return j;
}
#endif

#ifndef MEMORY_BARRIER
#define MEMORY_BARRIER() (void)0
#endif

#ifndef PREFETCH_FOR_WRITE
#define PREFETCH_FOR_WRITE(p) (void)0
#define PREFETCH_FOR_READ(p) (void)0
#endif

#endif /*RDF_MEMORY_H_INCLUDED*/
