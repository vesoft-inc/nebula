if(ENABLE_IncludeWhatYouUse)
  find_program(IncludeWhatYouUse include-what-you-use)
  if(IncludeWhatYouUse)
    message("use include-what-you-use")
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE
      "include-what-you-use;-Xiwyu;--transitive_includes_only")
  else()
    message(STATUS "iwyu requested but executable not found")
  endif()
endif()

