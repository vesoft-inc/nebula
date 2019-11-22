# These configuration files are for reference to generate your own customized ones.
# Thus, they are installed as read-only, even for the owner.
install(
    FILES
        nebula-graphd.conf.default
        nebula-storaged.conf.default
        nebula-metad.conf.default
    PERMISSIONS
        OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        etc
)

macro(nebula_install_conf conf)
    if(NOT EXISTS ${CMAKE_INSTALL_PREFIX}/etc/${conf})
        install(
            FILES
                ${conf}.default
            PERMISSIONS
                OWNER_READ
                OWNER_WRITE
                GROUP_READ
                WORLD_READ
            DESTINATION
                etc
            RENAME
                ${conf}
        )
    endif()
endmacro()

nebula_install_conf(nebula-graphd.conf)
nebula_install_conf(nebula-metad.conf)
nebula_install_conf(nebula-storaged.conf)
