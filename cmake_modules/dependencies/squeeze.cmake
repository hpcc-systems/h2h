#Debian 6.x
IF ( BUILD_WEBHDFS_VER )
	set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform, curl")
ELSE ()
    set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform, hadoop, openjdk-6-jre")
ENDIF ()
