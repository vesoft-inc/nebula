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
