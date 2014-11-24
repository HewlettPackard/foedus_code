

# This code uses boost::math for gamma distribution. It's header-only.
find_package(Boost)

if(Boost_FOUND)
  include_directories( ${Boost_INCLUDE_DIRS} )
  add_executable(lda lda.cpp)
  target_link_libraries(lda ${EXPERIMENT_LIB} gflags-static)
else()
  message(STATUS "Boost library is not found. FOEDUS itself doesn't require it, but the graph LDA")
  message(STATUS " experiment uses it. Disabled the graph LDA experiment. ")
endif()

configure_file(doc_word_count_nips.tsv counts.tsv COPYONLY)
configure_file(vocab.nips.txt dictionary.txt COPYONLY)
