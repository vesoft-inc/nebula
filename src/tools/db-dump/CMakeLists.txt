nebula_add_executable(
    NAME
        db_dump
    SOURCES
        DbDumpTool.cpp
        DbDumper.cpp
)

install(
    TARGETS
        db_dump
    DESTINATION
        bin
)
