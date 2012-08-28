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

project (hpccsystems-hdfsconnector)
cmake_minimum_required (VERSION 2.6)

SET ( HPCC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE (${HPCC_SOURCE_DIR}/version.cmake)

SET ( CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules")
#SET ( EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/bin" )
SET ( PRODUCT_PREFIX "hpccsystems" )

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

if ( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
else( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
endif ( ${ARCH64BIT} EQUAL 1 )

SET ( CPACK_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    set(CPACK_STRIP_FILES TRUE)
endif()

SET ( CPACK_INSTALL_CMAKE_PROJECTS "${CMAKE_CURRENT_BINARY_DIR};hdfsconnector;ALL;/")

if ( CMAKE_SYSTEM MATCHES Linux )
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

    message ( "-- Auto Detecting Packaging type")
    message ( "-- distro uses ${packageManagement}, revision is ${packageRevisionArch}" )

    if ( ${packageManagement} STREQUAL "DEB" )
        set(CPACK_PACKAGE_FILE_NAME     "${CMAKE_PROJECT_NAME}-${version}-${stagever}${packageRevisionArch}")
    elseif ( ${packageManagement} STREQUAL "RPM" )
        set(CPACK_PACKAGE_FILE_NAME     "${CMAKE_PROJECT_NAME}-${version}-${stagever}.${packageRevisionArch}")
    else()
        set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${version}-${stagever}${CPACK_SYSTEM_NAME}")
    endif ()

endif ( CMAKE_SYSTEM MATCHES Linux )

MESSAGE ("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")

set( CPACK_SOURCE_GENERATOR TGZ )

###
## CPack commands in this section require cpack 2.8.1 to function.
## When using cpack 2.8.1, the command "make package" will create
## an RPM.
###

if (NOT "${CMAKE_VERSION}" VERSION_LESS "2.8.1")
    if ( CMAKE_SYSTEM MATCHES Linux )
        if ( ${packageManagement} STREQUAL "DEB" )
            if ("${CMAKE_VERSION}" VERSION_EQUAL "2.8.2")
                message("WARNING: CMAKE 2.8.2  would not build DEB package")
            else ()
                set ( CPACK_GENERATOR "${packageManagement}" )
                message("-- Will build DEB package")
                ###
                ## CPack instruction required for Debian
                ###
                message ("-- Packing BASH installation files")
                set ( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst;${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm;${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
            endif ()

        elseif ( ${packageManagement} STREQUAL "RPM" )
            set ( CPACK_GENERATOR "${packageManagement}" )
            ###
            ## CPack instruction required for RPM
            ###
            message("-- Will build RPM package")
            message ("-- Packing BASH installation files")
            set ( CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst" )

            set ( CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm" )
            set ( CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
                else()
            message("WARNING: Unsupported package ${packageManagement}.")
        endif ()

    endif ( CMAKE_SYSTEM MATCHES Linux )
    if ( EXISTS ${CMAKE_MODULE_PATH}/dependencies/${packageRevision}.cmake )
        include( ${CMAKE_MODULE_PATH}/dependencies/${packageRevision}.cmake )
    else()
        message("-- WARNING: DEPENDENCY FILE FOR ${packageRevision} NOT FOUND, Using deps template.")
        include( ${CMAKE_MODULE_PATH}/dependencies/template.cmake )
    endif()
else()
    message("WARNING: CMAKE 2.8.1 or later required to create RPMs from this project")
endif()

if ( NOT MAKE_DOCS_ONLY )

    add_subdirectory (ecl)

    #IF TARBALLED_HADOOP_PATH IS NOT PASSED IN, DEFAULT TO /usr/local/hadoop
    #AT BUILD TIME THIS PATH WILL BE USED TO SEARCH FOR:
    # ${TARBALLED_HADOOP_PATH}/src/c++/libhdfs/hdfs.h
    # ${TARBALLED_HADOOP_PATH}/c++/${osdir}/libhdfs.so

    #AT RUN TIME THIS PATH WILL BE USED TO SEARCH FOR:
    # ${TARBALLED_HADOOP_PATH}/conf
    # ${TARBALLED_HADOOP_PATH}/*.jar
    # ${TARBALLED_HADOOP_PATH}/lib/*.jar

    #####################################################
    # hadoop install packages layout files in this manner:
    # conf files  -> /etc/hadoop
    # *.jar files -> /usr/share/hadoop & /usr/share/hadoop/lib
    # hdfslib     -> /usr/lib or /usr/lib64
    #####################################################

    option(TARBALLED_HADOOP_PATH "Set the Hadoop path.")
    if( NOT TARBALLED_HADOOP_PATH )
        SET (TARBALLED_HADOOP_PATH "/usr/local/hadoop")
    endif()

    #set other paths
    #generate config for script.
    #add script processor for vars.

    SET ( HPCC_ETC_DIR "${CMAKE_INSTALL_PREFIX}/${OSSDIR}/etc")
    SET ( HPCC_CONF_DIR "${CMAKE_INSTALL_PREFIX}/${OSSDIR}${CONFIG_DIR}")
    SET ( HDFSCONN_CONF_FILE "hdfsconnector.conf" )
    SET ( HDFSCONN_EXE_NAME "hdfsconnector" )
    SET ( HDFSCONN_EXE_PATH "${EXEC_PATH}/${HDFSCONN_EXE_NAME}")

    SET ( HADOOP_STD_CONF_PATH "/etc/hadoop" )
    SET ( HADOOP_STD_SHARE_PATH "/usr/share/hadoop" )

    find_package(JNI REQUIRED)
    find_package(LIBHDFS REQUIRED)

    configure_file("hdfsconnector.conf.in" "hdfsconnector.conf")
    configure_file("hdfspipe.in" "hdfspipe" @ONLY )

    SET ( SRC hdfsconnector.cpp hdfsconnector.hpp)

    include_directories (
                          ${CMAKE_BINARY_DIR}
                          ${CMAKE_BINARY_DIR}/oss
                          ${JNI_INCLUDE_DIRS}
                          ${JAVA_INCLUDE_PATH}
                          ${JAVA_INCLUDE_PATH2}
                          ${LIBHDFS_INCLUDE_DIR}  
                         )

    HPCC_ADD_EXECUTABLE( ${HDFSCONN_EXE_NAME} ${SRC} )

    SET ( INSTALLDIR "${OSSDIR}/bin")
    Install ( TARGETS hdfsconnector DESTINATION ${INSTALLDIR} COMPONENT Runtime)
    Install ( PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/hdfspipe DESTINATION ${INSTALLDIR} COMPONENT Runtime )
    Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/hdfsconnector.conf DESTINATION ${HPCC_CONF_DIR} COMPONENT Runtime )

    target_link_libraries ( hdfsconnector
                            ${JAVA_JVM_LIBRARY}
                            ${LIBHDFS_LIBRARIES}
                          )

    INCLUDE(CPack)
endif()
add_subdirectory(docs)
