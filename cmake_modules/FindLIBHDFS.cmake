################################################################################
#    Copyright (C) 2011 HPCC Systems.
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


# - Try to find the LIBHDFS xml library
# Once done this will define
#
#  LIBHDFS_FOUND - system has the LIBHDFS library
#  LIBHDFS_INCLUDE_DIR - the LIBHDFS include directory
#  LIBHDFS_LIBRARIES - The libraries needed to use LIBHDFS

if(HADOOP_VER GREATER 2.1)
	ADD_DEFINITIONS(-DHADOOP_GT_21)
endif()

if (NOT LIBHDFS_FOUND)
  IF (WIN32)
    SET (libhdfs_libs "hdfs" "libhdfs")
  ELSE()
    SET (libhdfs_libs "hdfs" "libhdfs")
  ENDIF()

  IF (NOT USE_NATIVE_LIBRARIES)
    MESSAGE("-- Searching for libhdfs: NOT using native libraries")

     IF ( SRC_BUILT_HADOOP_PATH STREQUAL "" )
	MESSAGE("---SRC_BUILT_HADOOP_PATH not specified")
     ELSE ()
        LIST (APPEND POSSILE_PATHS
            "${SRC_BUILT_HADOOP_PATH}/hadoop-${HADOOP_VER}-src/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-${HADOOP_VER}/include"
            "${SRC_BUILT_HADOOP_PATH}/hadoop-${HADOOP_VER}-src/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-${HADOOP_VER}/lib/native/"
           )
     ENDIF ()

     IF ( TARBALLED_HADOOP_PATH STREQUAL "" )
        MESSAGE("---TARBALLED_HADOOP_PATH not specified")
     ELSE ()
        LIST (APPEND POSSILE_PATHS
             "${TARBALLED_HADOOP_PATH}/src/c++/libhdfs"
             "${TARBALLED_HADOOP_PATH}/c++/${hdfsosdir}/lib"
           )
     ENDIF()

    MESSAGE("--  Exploring these paths to find libhdfs and hdfs.h: ${POSSILE_PATHS}.")

    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (hdfsosdir "Linux-amd64-64")
      ELSE()
        SET (hdfsosdir "Linux-i386-32")
      ENDIF()
    ELSEIF(WIN32)
      SET (hdfsosdir "lib")
    ELSE()
      SET (hdfsosdir "unknown")
    ENDIF()
    IF (NOT ("${hdfsosdir}" STREQUAL "unknown"))
      FIND_PATH (LIBHDFS_INCLUDE_DIR NAMES hdfs.h PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)
      FIND_LIBRARY (LIBHDFS_LIBRARIES NAMES ${libhdfs_libs} PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)
    ENDIF()
  ELSE()
    MESSAGE("-- Searching for libhdfs using native libraries location.")
    MESSAGE("--  Will use tarballed location (${TARBALLED_HADOOP_PATH}) to find header file.")

    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (libhdfspath "/usr/lib64")
      ELSE()
        SET (libhdfspath "/usr/lib")
      ENDIF()
    ELSEIF(WIN32)
      SET (libhdfspath "lib")
    ELSE()
      SET (libhdfspath "unknown")
    ENDIF()

    # standard install doesn't include header, look in untared location
    FIND_PATH (LIBHDFS_INCLUDE_DIR NAMES hdfs.h PATHS "${TARBALLED_HADOOP_PATH}/src/c++/libhdfs" )

    IF (NOT ("${libhdfspaths}" STREQUAL "unknown"))
         FIND_LIBRARY (LIBHDFS_LIBRARIES NAMES ${libhdfs_libs} PATHS "${libhdfspath}")
    ENDIF()

  ENDIF()
  INCLUDE(FindPackageHandleStandardArgs)

  find_package_handle_standard_args(Libhdfs DEFAULT_MSG
    LIBHDFS_LIBRARIES
    LIBHDFS_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(LIBHDFS_INCLUDE_DIR LIBHDFS_LIBRARIES )

    IF (LIBHDFS_FOUND)
         MESSAGE ("---LIBHDFS ${LIBHDFS_LIBRARIES} found.")
    ELSE()
        MESSAGE ("---LIBHDFS library not found.")
    ENDIF()

    IF (LIBHDFS_INCLUDE_DIR)
         MESSAGE ("---LIBHDFS ${LIBHDFS_INCLUDE_DIR} found.")
    ELSE()
        MESSAGE ("---HDFS header not found.")
    ENDIF()

ENDIF()
