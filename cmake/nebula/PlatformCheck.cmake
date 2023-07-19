if(${CMAKE_SYSTEM_NAME} MATCHES "(Darwin|FreeBSD|Windows)")
	MESSAGE(FATAL_ERROR "Only Linux is supported")
endif ()
