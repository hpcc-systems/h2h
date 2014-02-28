###############################################################################
#    Copyright (C) 2012 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################
CMAKE_MINIMUM_REQUIRED (VERSION 2.6)
SET ( PRODUCT_PREFIX "hpccsystems" )

option(BUILD_WEBHDFS_VER "Build WebHDFS version of HDFSConnector." ON)

IF( BUILD_WEBHDFS_VER )
    SET (HDFS_CONNECTOR_TYPE "webhdfsconnector")
ELSE()
    SET (HDFS_CONNECTOR_TYPE "libhdfsconnector")
ENDIF()

PROJECT (${PRODUCT_PREFIX}-${HDFS_CONNECTOR_TYPE})

SET ( HPCC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE (${HPCC_SOURCE_DIR}/version.cmake)

SET ( CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules")

INCLUDE(${CMAKE_MODULE_PATH}/optionDefaults.cmake)
INCLUDE(${CMAKE_MODULE_PATH}/commonSetup.cmake)
INCLUDE(InstallRequiredSystemLibraries)

SET ( CPACK_PACKAGE_VERSION_MAJOR ${majorver})
SET ( CPACK_PACKAGE_VERSION_MINOR ${minorver})
SET ( CPACK_PACKAGE_VERSION_PATCH ${point}${stagever})
SET ( CPACK_PACKAGE_CONTACT "HPCCSystems <ossdevelopment@lexisnexis.com>" )
SET ( CPACK_SOURCE_GENERATOR TGZ )
SET ( CPACK_RPM_PACKAGE_VERSION "${projname}")
SET ( CPACK_RPM_PACKAGE_RELEASE "${version}${stagever}")

IF ( ${ARCH64BIT} EQUAL 1 )
    SET ( CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
ELSE( ${ARCH64BIT} EQUAL 1 )
    SET ( CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
ENDIF ( ${ARCH64BIT} EQUAL 1 )

SET ( CPACK_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")

IF ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    SET (CPACK_STRIP_FILES TRUE)
ENDIF()

SET ( CPACK_INSTALL_CMAKE_PROJECTS "${CMAKE_CURRENT_BINARY_DIR};hdfsconnector;ALL;/")

IF ( CMAKE_SYSTEM MATCHES Linux )
    EXECUTE_PROCESS (
                COMMAND ${CMAKE_MODULE_PATH}/distrocheck.sh
                    OUTPUT_VARIABLE packageManagement
                        ERROR_VARIABLE  packageManagement
                )
    EXECUTE_PROCESS (
                COMMAND ${CMAKE_MODULE_PATH}/getpackagerevisionarch.sh
                    OUTPUT_VARIABLE packageRevisionArch
                        ERROR_VARIABLE  packageRevisionArch
                )
    EXECUTE_PROCESS (
                COMMAND ${CMAKE_MODULE_PATH}/getpackagerevisionarch.sh --noarch
                    OUTPUT_VARIABLE packageRevision
                        ERROR_VARIABLE  packageRevision
                )

    MESSAGE ( "-- Auto Detecting Packaging type")
    MESSAGE ( "-- distro uses ${packageManagement}, revision is ${packageRevisionArch}" )

    IF ( ${packageManagement} STREQUAL "DEB" )
        SET(CPACK_PACKAGE_FILE_NAME     "${CMAKE_PROJECT_NAME}-${version}-${stagever}${packageRevisionArch}")
    ELSEIF ( ${packageManagement} STREQUAL "RPM" )
        SET(CPACK_PACKAGE_FILE_NAME     "${CMAKE_PROJECT_NAME}-${version}-${stagever}.${packageRevisionArch}")
    ELSE()
        SET(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${version}-${stagever}${CPACK_SYSTEM_NAME}")
    ENDIF ()

ENDIF ( CMAKE_SYSTEM MATCHES Linux )

MESSAGE ("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")

SET ( CPACK_SOURCE_GENERATOR TGZ )

###
## CPack commands in this section require cpack 2.8.1 to function.
## When using cpack 2.8.1, the command "make package" will create
## an RPM.
###

IF (NOT "${CMAKE_VERSION}" VERSION_LESS "2.8.1")
    IF ( CMAKE_SYSTEM MATCHES Linux )
        IF ( ${packageManagement} STREQUAL "DEB" )
            IF ("${CMAKE_VERSION}" VERSION_EQUAL "2.8.2")
                MESSAGE("WARNING: CMAKE 2.8.2  would not build DEB package")
            ELSE ()
                SET ( CPACK_GENERATOR "${packageManagement}" )
                MESSAGE("-- Will build DEB package")
                ###
                ## CPack instruction required for Debian
                ###
                MESSAGE ("-- Packing BASH installation files")
                SET ( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/postinst;${CMAKE_CURRENT_BINARY_DIR}/prerm;${CMAKE_CURRENT_BINARY_DIR}/postrm" )
            ENDIF ()

        ELSEIF ( ${packageManagement} STREQUAL "RPM" )
            SET ( CPACK_GENERATOR "${packageManagement}" )
            ###
            ## CPack instruction required for RPM
            ###
            MESSAGE("-- Will build RPM package")
            MESSAGE ("-- Packing BASH installation files")
            SET ( CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/postinst" )
            SET ( CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/prerm" )
            SET ( CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/postrm" )
        ELSE()
            MESSAGE("WARNING: Unsupported package ${packageManagement}.")
        ENDIF ()

    ENDIF ( CMAKE_SYSTEM MATCHES Linux )
    IF ( EXISTS ${CMAKE_MODULE_PATH}/dependencies/${packageRevision}.cmake )
        INCLUDE( ${CMAKE_MODULE_PATH}/dependencies/${packageRevision}.cmake )
    ELSE()
        MESSAGE("-- WARNING: DEPENDENCY FILE FOR ${packageRevision} NOT FOUND, Using deps template.")
        INCLUDE( ${CMAKE_MODULE_PATH}/dependencies/template.cmake )
    ENDIF()
ELSE()
    MESSAGE("WARNING: CMAKE 2.8.1 or later required to create RPMs from this project")
ENDIF()

IF ( NOT MAKE_DOCS_ONLY )
    ADD_SUBDIRECTORY (ecl)

    SET ( HPCC_ETC_DIR "${CMAKE_INSTALL_PREFIX}/${OSSDIR}/etc")
    SET ( HPCC_CONF_DIR "${CMAKE_INSTALL_PREFIX}/${OSSDIR}${CONFIG_DIR}")
    SET ( HDFSCONN_CONF_FILE "hdfsconnector.conf" )
    SET ( INSTALLDIR "${OSSDIR}/bin")

    SET ( HADOOP_STD_CONF_PATH "/etc/hadoop" )
    SET ( HADOOP_STD_SHARE_PATH "/usr/share/hadoop" )

    OPTION(TARBALLED_HADOOP_PATH "Set the Hadoop path.")
    IF( NOT TARBALLED_HADOOP_PATH )
        SET (TARBALLED_HADOOP_PATH "/usr/local/hadoop")
    ENDIF()

    CONFIGURE_FILE("${HPCC_SOURCE_DIR}/postinst.in" "postinst")
    MESSAGE ("-- Building ${HDFS_CONNECTOR_TYPE} --")
    IF ( BUILD_WEBHDFS_VER )
        SET ( HDFSCONN_EXE_NAME ${HDFS_CONNECTOR_TYPE} )
        SET ( HDFSCONN_EXE_PATH "${EXEC_PATH}/${HDFSCONN_EXE_NAME}")

        FIND_PACKAGE(CURL REQUIRED)

        SET ( SRC hdfsconnector.hpp webhdfsconnector.cpp webhdfsconnector.hpp)

        INCLUDE_DIRECTORIES ( ${CMAKE_BINARY_DIR} ${CURL_INCLUDE_DIR} )
        HPCC_ADD_EXECUTABLE( ${HDFSCONN_EXE_NAME} ${SRC} )

        INSTALL ( TARGETS ${HDFSCONN_EXE_NAME} DESTINATION ${INSTALLDIR} COMPONENT Runtime)

        MESSAGE("-- LIBHDFDSCONNECTOR link libs:")
        MESSAGE("--     ${HDFSCONN_EXE_NAME}")
        MESSAGE("--      ${CURL_LIBRARY}")

        TARGET_LINK_LIBRARIES ( ${HDFSCONN_EXE_NAME} ${CURL_LIBRARY} )

    ELSE ()
        SET ( HDFSCONN_EXE_NAME ${HDFS_CONNECTOR_TYPE} )
        SET ( HDFSCONN_EXE_PATH "${EXEC_PATH}/${HDFSCONN_EXE_NAME}")

        FIND_PACKAGE(JNI REQUIRED)
        FIND_PACKAGE(LIBHDFS REQUIRED)

        GET_FILENAME_COMPONENT(H2H_LIBJVM_PATH ${JAVA_JVM_LIBRARY}  PATH)
        GET_FILENAME_COMPONENT(H2H_LIBHDFS_PATH ${LIBHDFS_LIBRARIES}  PATH)

        SET ( SRC hdfsconnector.hpp libhdfsconnector.cpp libhdfsconnector.hpp)

        INCLUDE_DIRECTORIES (
                      ${CMAKE_BINARY_DIR}
                      ${CMAKE_BINARY_DIR}/oss
                      ${JNI_INCLUDE_DIRS}
                      ${JAVA_INCLUDE_PATH}
                      ${JAVA_INCLUDE_PATH2}
                      ${LIBHDFS_INCLUDE_DIR}
                     )

        HPCC_ADD_EXECUTABLE( ${HDFSCONN_EXE_NAME} ${SRC} )

        INSTALL ( TARGETS ${HDFSCONN_EXE_NAME} DESTINATION ${INSTALLDIR} COMPONENT Runtime)

        MESSAGE("-- LIBHDFDSCONNECTOR link libs:")
        MESSAGE("--     ${JAVA_JVM_LIBRARY}")
        MESSAGE("--     ${LIBHDFS_LIBRARIES}")
        MESSAGE("--     ${HDFSCONN_EXE_NAME}")

        TARGET_LINK_LIBRARIES ( ${HDFSCONN_EXE_NAME}
                                ${JAVA_JVM_LIBRARY}
                                ${LIBHDFS_LIBRARIES}
                              )
    ENDIF()

    CONFIGURE_FILE("${HPCC_SOURCE_DIR}/${HDFSCONN_CONF_FILE}.in" ${HDFSCONN_CONF_FILE})
    CONFIGURE_FILE("${HPCC_SOURCE_DIR}/hdfspipe.in" "${CMAKE_CURRENT_BINARY_DIR}/hdfspipe" @ONLY )

    INSTALL ( PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/hdfspipe DESTINATION ${INSTALLDIR} COMPONENT Runtime )
    INSTALL ( FILES ${CMAKE_CURRENT_BINARY_DIR}/${HDFSCONN_CONF_FILE} DESTINATION ${HPCC_CONF_DIR}/rpmnew COMPONENT Runtime )
    INSTALL ( PROGRAMS ${HPCC_SOURCE_DIR}/install-h2h-init DESTINATION ${HPCC_ETC_DIR}/init.d COMPONENT Runtime )

    INCLUDE(CPack)
ENDIF()

ADD_SUBDIRECTORY(docs)
