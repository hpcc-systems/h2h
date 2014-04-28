################################################################################
#    Copyright (C) 2014 HPCC Systems.
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

# This is a template of what is needed to create a dependency file for your distribution.

# Steps to follow.
# 1. Copy template.cmake to <distribution name>.cmake
#       ex. Ubuntu 11.04      natty.cmake
#       ex. Centos 5.x            el5.cmake
#
# 2. Add a DEPENDS line based on the package system of your distribution.
#       ex. set ( CPACK_DEBIAN_PACKAGE_DEPENDS "libicu,libboost-regex1.42.0")
#       ex. set ( CPACK_RPM_PACKAGE_REQUIRES "libicu, boost")
#
# 3. Save your changes and create a build from your build directory with `make package`
