#Ubuntu 13.10
IF ( BUILD_WEBHDFS_VER )
	set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform, libcurl3, libcurl3-nss")
ELSE ()
    set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform, hadoop, openjdk-6-jre")
ENDIF ()
