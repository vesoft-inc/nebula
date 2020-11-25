# Is abi 11
if (${DISABLE_CXX11_ABI})
    message(STATUS "Set D_GLIBCXX_USE_CXX11_ABI to 0")
    add_compile_options(-D_GLIBCXX_USE_CXX11_ABI=0)
else()
    message(STATUS "Set D_GLIBCXX_USE_CXX11_ABI to 1")
    add_compile_options(-D_GLIBCXX_USE_CXX11_ABI=1)
endif()
