dnl Process this file with autoconf to produce a configure script.

AC_INIT(install-sh)
AC_PREREQ([2.50])
AC_CONFIG_HEADER(config.h)

m4_include([../ac_swi_c.m4])

AC_CHECK_SIZEOF(long)
AC_CHECK_SIZEOF(void*)

dnl This fragment is from the glibc configure script
dnl with modification to fit different platforms

if test "$GCC" = "yes"; then
  AC_CACHE_CHECK(whether __attribute__((visibility())) is supported,
                 libc_cv_visibility_attribute,
                 [cat > conftest.c <<EOF
                  int foo __attribute__ ((visibility ("hidden"))) = 1;
EOF
                  libc_cv_visibility_attribute=no
                  if ${CC-cc} -Werror -S conftest.c -o conftest.s >/dev/null 2>&1; then
                    if grep -E "\.(private_extern|hidden).*foo" conftest.s >/dev/null; then
                      libc_cv_visibility_attribute=yes
                    fi
                  fi
                  rm -f conftest.[cs]
                 ])
  if test $libc_cv_visibility_attribute = yes; then
    AC_DEFINE(HAVE_VISIBILITY_ATTRIBUTE, 1,
              [Define if __attribute__ visibility is supported])
  fi
fi

AC_CHECK_FUNCS(wcsdup wcscasecmp)

AC_OUTPUT(Makefile)




