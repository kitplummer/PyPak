#!/usr/bin/env python

#
# Example program for retrieving the table definitions from a data logger
#
# Update the file pakbus.conf to your local settings first!
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

import socket
import sys
import pakbus
from bintools import str2int

#
# Initialize parameters
#

# Parse command line arguments
import optparse
parser = optparse.OptionParser()
parser.add_option('-c', '--config', help = 'read configuration from FILE [default: %default]', metavar = 'FILE', default = 'pakbus.conf')
(options, args) = parser.parse_args()

# Read configuration file
import ConfigParser, StringIO
cf = ConfigParser.SafeConfigParser()
print 'configuration read from %s' % cf.read(options.config)

# Data logger PakBus Node Id
NodeId = str2int(cf.get('pakbus', 'node_id'))
# My PakBus Node Id
MyNodeId = str2int(cf.get('pakbus', 'my_node_id'))

# Open socket
s = pakbus.open_socket(cf.get('pakbus', 'host'), cf.getint('pakbus', 'port'), cf.getint('pakbus', 'timeout'))

# check if remote node is up
msg = pakbus.ping_node(s, NodeId, MyNodeId)
if not msg:
    raise Warning('no reply from PakBus node 0x%.3x' % NodeId)

#
# Main program
#

# Get table definition structure
FileData, RespCode = pakbus.fileupload(s, NodeId, MyNodeId, FileName = '.TDF')

if FileData:
    tabledef = pakbus.parse_tabledef(FileData)
    for tableno in range(1, len(tabledef) + 1):
        print 'Table %d: %s' % (tableno, tabledef[tableno - 1]['Header']['TableName'])
        print 'Table signature: 0x%X' % tabledef[tableno - 1]['Signature']
        print 'Header:', tabledef[tableno - 1]['Header']
        for fieldno in range(1, len(tabledef[tableno - 1]['Fields']) + 1):
            print 'Field %d:' % (fieldno), tabledef[tableno - 1]['Fields'][fieldno - 1]
        print

# say good bye
pkt = pakbus.pkt_bye_cmd(NodeId, MyNodeId)
pakbus.send(s, pkt)

# close socket
s.close()
