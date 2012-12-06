#Debian 5.x
IF ( BUILD_WEBHDFS_VER )
	set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform, libcurl3 ")
ELSE ()
    set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform, hadoop, openjdk-6-jre")
ENDIF ()
