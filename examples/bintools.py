#
# Python function library for handling binary data
#

#
# (c) 2009 Dietrich Feist, Max Planck Institute for Biogeochemistry, Jena Germany
#          Email: dfeist@bgc-jena.mpg.de
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#
# Convert binary string to hex output
#
def ByteToHex(byteStr):
    return ' '.join( [ "%02X" % ord(x) for x in byteStr ] )


#
# Convert byte array to MSB integer value of arbitrary size
#
def ByteToInt(byteStr):
    int = 0
    for x in byteStr:
        int = (int<<8) + ord(x)
    return int


#
# Convert a decimal or hex number from string to int
#
def str2int(S):

    S = S.strip()
    # Check for hex number
    try:
        S.index('0x', 0, 2)
    except ValueError:
        base = 10
    else:
        base = 16

    return int(S, base)
