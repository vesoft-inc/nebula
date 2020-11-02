install(
    FILES
        gflags.json
        date_time_zonespec.csv
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        share/resources
    COMPONENT
        common
)
