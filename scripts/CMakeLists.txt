install(
    FILES
        nebula-graphd.service
        nebula-storaged.service
        nebula-metad.service
        nebula.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
)

install(
    FILES
        utils.sh
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        scripts
)
