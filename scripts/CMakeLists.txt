install(
    FILES nebula-graphd.service nebula-storaged.service nebula-metad.service
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    DESTINATION scripts
)
