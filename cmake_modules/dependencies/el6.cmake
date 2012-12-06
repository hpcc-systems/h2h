#centOS Redhat 6.x
IF ( BUILD_WEBHDFS_VER )
	set ( CPACK_RPM_PACKAGE_REQUIRES "hpccsystems-platform, curl")
ELSE ()
    set ( CPACK_RPM_PACKAGE_REQUIRES "hpccsystems-platform, hadoop, java-1.6.0-openjdk")
ENDIF ()
