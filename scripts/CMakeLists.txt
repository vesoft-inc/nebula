install(
    FILES
        nebula-graphd.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        graph
)

install(
    FILES
        nebula-storaged.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        storage
)

install(
    FILES
        nebula-metad.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        meta
)

install(
    FILES
        nebula.service
        services.sh
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        common
)

install(
    FILES
        utils.sh
        meta.hosts
        storage.hosts
        graph.hosts
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        common
)
