cmake_minimum_required(VERSION 3.5)
project(swipl-semweb)

include("../cmake/PrologPackage.cmake")
include(GCCBuiltins)

AC_CHECK_FUNCS(wcsdup wcscasecmp)

check_type_size("long" SIZEOF_LONG)
check_type_size("void *" SIZEOF_VOIDP)

configure_file(config.h.cmake config.h)

swipl_plugin(
    turtle
    C_SOURCES turtle.c murmur.c
    C_LIBS
    PL_LIB_SUBDIR semweb
    PL_LIBS turtle.pl rdf_turtle_write.pl rdf_prefixes.pl)

swipl_plugin(
    ntriples
    C_SOURCES ntriples.c
    C_LIBS
    PL_LIB_SUBDIR semweb
    PL_LIBS rdf_ntriples.pl)

add_custom_target(semweb)
add_dependencies(semweb turtle ntriples)

if(MULTI_THREADED)
set(SWIPL_SEMWEB_DATA
    rdfs.rdfs dc.rdfs eor.rdfs owl.owl rdf_library.ttl)
prepend(SWIPL_SEMWEB_DATA ${CMAKE_CURRENT_SOURCE_DIR} ${SWIPL_SEMWEB_DATA})

if(HAVE_LIBATOMIC)
  set(RDFDB_CLIBS atomic)
else()
  set(RDFDB_CLIBS)
endif()

swipl_plugin(
    rdf_db
    C_SOURCES rdf_db.c atom.c md5.c atom_map.c debug.c
	      hash.c murmur.c query.c resource.c error.c skiplist.c
	      snapshot.c xsd.c
    C_LIBS ${RDFDB_CLIBS}
    PL_LIB_SUBDIR semweb
    PL_LIBS rdf_db.pl rdfs.pl rdf_edit.pl rdf_litindex.pl
	    rdf_persistency.pl rdf_turtle.pl rdf_cache.pl
            rdf_http_plugin.pl rdf_zlib_plugin.pl rdf_portray.pl
            rdf_compare.pl rdf_library.pl sparql_client.pl
            rdfa.pl rdf11.pl rdf11_containers.pl rdf_sandbox.pl)

install_src(pkg_semweb_data
	    FILES ${SWIPL_SEMWEB_DATA}
	    DESTINATION ${SWIPL_INSTALL_LIBRARY}/semweb)

add_dependencies(semweb turtle ntriples)
endif(MULTI_THREADED)


# FIXME: test_rdfa.pl is not used.  Was also the case in the
# autoconf version
test_libs(con litmap load ntriples rdf11 rdf_db
	  subprop turtle2 turtle
	  PACKAGES sgml clib zlib http plunit RDF
     PARENT_LIB
     TEST_FILES local_test.pl random_graph.pl search.pl dc.rdfs pce_graph.pl
     TEST_DIRS Tests)

pkg_doc(semweb
	LIBSUBDIR
	    semweb
	SOURCES
	    rdflib.doc
	SECTION
	    turtle.pl rdfa.pl rdf_ntriples.pl rdf_compare.pl rdf_portray.pl
	    sparql_client.pl
	SUBSECTION
	    rdf_cache.pl rdf11_containers.pl
	    SOURCE doc/rdfdb.md rdfdb.tex --rdfdb
	    SOURCE doc/rdf11.md rdf11.tex --rdf11)
