#
# Python function library for PAKBUS communication
#
# (c) 2009 Dietrich Feist, Max Planck Institute for Biogeochemistry, Jena Germany
#
# Licensed under the GNU General Public License
#
# References:
#
# [1] BMP5 Transparent Commands Manual, Rev. 9/08, Campbell Scientific Inc., 2008
# [2] PakBus Networking Guide for the CR10X, CR510, CR23X, and CR200 Series
#     and LoggerNet 2.1C, Rev. 3/05, Campbell Scientific Inc., 2004-2005
#
# $Id: pakbus.py 40 2009-12-14 19:45:46Z dfeist $
#


#
# Global imports
#
import struct
import string


#
# Global definitions
#
datatype = {
    #
    # data type summary table, check [1] Appendix A for details
    #
    # name        code        format        size
    #
    'Byte':     { 'code':  1, 'fmt': 'B',   'size': 1 },
    'UInt2':    { 'code':  2, 'fmt': '>H',  'size': 2 },
    'UInt4':    { 'code':  3, 'fmt': '>L',  'size': 4 },
    'Int1':     { 'code':  4, 'fmt': 'b',   'size': 1 },
    'Int2':     { 'code':  5, 'fmt': '>h',  'size': 2 },
    'Int4':     { 'code':  6, 'fmt': '>l',  'size': 4 },
    'FP2':      { 'code':  7, 'fmt': '>H',  'size': 2 },
    'FP3':      { 'code': 15, 'fmt': '3c',  'size': 3 },
    'FP4':      { 'code':  8, 'fmt': '4c',  'size': 4 },
    'IEEE4B':   { 'code':  9, 'fmt': '>f',  'size': 4 },
    'IEEE8B':   { 'code': 18, 'fmt': '>d',  'size': 8 },
    'Bool8':    { 'code': 17, 'fmt': 'B',   'size': 1 },
    'Bool':     { 'code': 10, 'fmt': 'B',   'size': 1 },
    'Bool2':    { 'code': 27, 'fmt': '>H',  'size': 2 },
    'Bool4':    { 'code': 28, 'fmt': '>L',  'size': 4 },
    'Sec':      { 'code': 12, 'fmt': '>l',  'size': 4 },
    'USec':     { 'code': 13, 'fmt': '6c',  'size': 6 },
    'NSec':     { 'code': 14, 'fmt': '>2l', 'size': 8 },
    'ASCII':    { 'code': 11, 'fmt': 's',   'size': None },
    'ASCIIZ':   { 'code': 16, 'fmt': 's',   'size': None },
    'Short':    { 'code': 19, 'fmt': '<h',  'size': 2 },
    'Long':     { 'code': 20, 'fmt': '<l',  'size': 4 },
    'UShort':   { 'code': 21, 'fmt': '<H',  'size': 2 },
    'ULong':    { 'code': 22, 'fmt': '<L',  'size': 4 },
    'IEEE4L':   { 'code': 24, 'fmt': '<f',  'size': 4 },
    'IEEE8L':   { 'code': 25, 'fmt': '<d',  'size': 8 },
    'SecNano':  { 'code': 23, 'fmt': '<2l', 'size': 8 },
}


#
# Global variables
#
if not vars().has_key('transact'):
    transact = 0     # Running 8-bit transaction counter (initialized only if it does not exist)


#
# Send packet over PakBus
#
# - add signature nullifier
# - quote \xBC and \xBD characters
# - frame packet with \xBD characters
#
def send(s, pkt):
    # s: socket object
    # pkt: unquoted, unframed PakBus packet (just header + message)
    frame = quote(pkt + calcSigNullifier(calcSigFor(pkt)))
    s.send('\xBD' + frame + '\xBD')


#
# Receive packet over PakBus
#
# - remove framing \xBD characters
# - unquote quoted \xBC and \xBD characters
# - check signature
#
def recv(s):
    # s: socket object
    pkt = ''
    byte = None
    while byte != '\xBD': byte = s.recv(1) # Read until first \xBD frame character
    while byte == '\xBD': byte = s.recv(1) # Read unitl first character other than \xBD
    while byte != '\xBD': # Read until next occurence of \xBD character
        pkt += byte
        byte = s.recv(1)
    pkt = unquote(pkt)  # Unquote quoted characters
    if calcSigFor(pkt): # Calculate signature (should be zero)
        return None     # Signature not zero!
    else:
        return pkt[:-2] # Strip last 2 signature bytes and return packet


#
# Generate new 8-bit transaction number
#
def newTranNbr():
    global transact
    transact += 1
    transact &= 0xFF
    return transact


################################################################################
#
# [1] section 1.3 PakBus Packet Headers
#
################################################################################

#
# Generate PakBus header
#
def PakBus_hdr(DstNodeId, SrcNodeId, HiProtoCode, ExpMoreCode = 0x2, LinkState = 0xA, Priority = 0x1, HopCnt = 0x0, DstPhyAddr = None, SrcPhyAddr = None):
    # DstNodeId:   Node ID of the message destination
    # SrcNodeId:   Node ID of the message source
    # HiProtoCode: Higher level protocol code (4 bits); 0x0: PakCtrl, 0x1: BMP5
    # ExpMoreCode: Whether client should expect another packet (2 bits)
    # LinkState:   Link state (4 bits)
    # Priority:    Message priority on the network (2 bits)
    # HopCnt:      Number of hops to destination (4 bits)
    # DstPhyAddr:  Address where this packet is going (12 bits)
    # SrcPhyAddr:  Address of the node that sent the packet (12 bits)

    # set default physical addresses equal to node ID
    if not DstPhyAddr: DstPhyAddr = DstNodeId
    if not SrcPhyAddr: SrcPhyAddr = SrcNodeId
    # bitwise encoding of header fields
    hdr = struct.pack('>4H',
        (LinkState & 0xF) << 12   | (DstPhyAddr & 0xFFF),
        (ExpMoreCode & 0x3) << 14 | (Priority & 0x3) << 12 | (SrcPhyAddr & 0xFFF),
        (HiProtoCode & 0xF) << 12 | (DstNodeId & 0xFFF),
        (HopCnt & 0xF) << 12      | (SrcNodeId & 0xFFF)
    )
    return hdr


################################################################################
#
# [1] section 1.4 Encoding and Decoding Packets
#
################################################################################

#
# Calculate signature for PakBus packets
#
def calcSigFor(buff, seed = 0xAAAA):
    sig = seed
    for x in buff:
        x = ord(x)
        j = sig
        sig = (sig <<1) & 0x1FF
        if sig >= 0x100: sig += 1
        sig = ((((sig + (j >>8) + x) & 0xFF) | (j <<8))) & 0xFFFF
    return sig

#
# Calculate signature nullifier needed to create valid PakBus packets
#
def calcSigNullifier(sig):
    nulb = nullif = ''
    for i in 1,2:
        sig = calcSigFor(nulb, sig)
        sig2 = (sig<<1) & 0x1FF
        if sig2 >= 0x100: sig2 += 1
        nulb = chr((0x100 - (sig2 + (sig >>8))) & 0xFF)
        nullif += nulb
    return nullif

#
# Quote PakBus packet
#
def quote(pkt):
    pkt = string.replace(pkt, '\xBC', '\xBC\xDC') # quote \xBC characters
    pkt = string.replace(pkt, '\xBD', '\xBC\xDD') # quote \xBD characters
    return pkt

#
# Unquote PakBus packet
#
def unquote(pkt):
    pkt = string.replace(pkt, '\xBC\xDD', '\xBD') # unquote \xBD characters
    pkt = string.replace(pkt, '\xBC\xDC', '\xBC') # unquote \xBC characters
    return pkt


################################################################################
#
# [1] section 2.2 PakBus Control Packets (PakCtrl)
#
################################################################################

################################################################################
#
# [1] section 2.2.1 Deliverry Failure Message (MsgType 0x81)
#
################################################################################

#
# still missing ...
#


################################################################################
#
# [1] section 2.2.2 Hello Transaction (MsgType 0x09 & 0x89)
#
################################################################################

#
# Create Hello Command packet
#
def pkt_hello_cmd(DstNodeId, SrcNodeId, IsRouter = 0x00, HopMetric = 0x02, VerifyIntv = 1800):
    # DstNodeId:   Destination node ID (12-bit int)
    # SrcNodeId:   Source node ID (12-bit int)
    # IsRouter:    Flag if source node is a router (default: 0)
    # HopMetric:   Worst case interval to complete transaction (default: 0x02 -> 5 s)
    # VerifyIntv:  Link verification interval in seconds (default: 30 minutes)

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x0, 0x1, 0x9) # PakBus Control Packet
    msg = encode_bin(['Byte', 'Byte', 'Byte', 'Byte', 'UInt2'], [0x09, TranNbr, IsRouter, HopMetric, VerifyIntv])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Create Hello Response packet
#
def pkt_hello_response(DstNodeId, SrcNodeId, TranNbr, IsRouter = 0x00, HopMetric = 0x02, VerifyIntv = 1800):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # TranNbr:      Transaction number from received hello command packet
    # IsRouter:     Flag if source node is a router (default: 0)
    # HopMetric:    Worst case interval to complete transaction (default: 0x02 -> 5 s)
    # VerifyIntv:   Link verification interval in seconds (default: 30 minutes)

    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x0) # PakBus Control Packet
    msg = encode_bin(['Byte', 'Byte', 'Byte', 'Byte', 'UInt2'], [0x89, TranNbr, IsRouter, HopMetric, VerifyIntv])
    pkt = hdr + msg
    return pkt

#
# Decode Hello Command/Response packet
#
def msg_hello(msg):
    # msg: decoded default message - must contain msg['raw']

    [msg['IsRouter'], msg['HopMetric'], msg['VerifyIntv']], size = decode_bin(['Byte', 'Byte', 'UInt2'], msg['raw'][2:])
    return msg


################################################################################
#
# [1] section 2.2.3 Hello Request Message (MsgType 0x0e)
#
################################################################################

#
# still missing ...
#


################################################################################
#
# [1] section 2.2.4 Bye Message (MsgType 0x0d)
#
################################################################################

#
# Create Bye Command packet
#
def pkt_bye_cmd(DstNodeId, SrcNodeId):
    # DstNodeId:   Destination node ID (12-bit int)
    # SrcNodeId:   Source node ID (12-bit int)

    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x0, 0x0, 0xB) # PakBus Control Packet
    msg = encode_bin(['Byte', 'Byte'], [0x0d, 0x0])
    pkt = hdr + msg
    return pkt


################################################################################
#
# [1] section 2.2.5 Get/Set String Settings Transactions (MsgType 0x07, 0x87, 0x08, & 0x88)
#
################################################################################

#
# still missing ... (only useful for CR200)
#


################################################################################
#
# [1] section 2.2.6.1 DevConfig Get Settings Message (MsgType 0x0f & 0x8f)
#
################################################################################

#
# Create DevConfig Get Settings Command packet
#
def pkt_devconfig_get_settings_cmd(DstNodeId, SrcNodeId, BeginSettingId = None, EndSettingId = None, SecurityCode = 0x0000):
    # DstNodeId:        Destination node ID (12-bit int)
    # SrcNodeId:        Source node ID (12-bit int)
    # BeginSettingId:   First setting for the datalogger to include in response
    # EndSettingId:     Last setting for the datalogger to include in response
    # SecurityCode:     16-bit security code (optional)

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x0) # PakBus Control Packet
    msg = encode_bin(['Byte', 'Byte'], [0x0f, TranNbr])
    if not BeginSettingId is None:
        msg += encode_bin(['UInt2'], [BeginSettingId])
        if not EndSettingId is None:
            msg += encode_bin(['UInt2'], [EndSettingId])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode DevConfig Get Settings Response packet
#
def msg_devconfig_get_settings_response(msg):
    # msg: decoded default message - must contain msg['raw']

    offset = 2
    [msg['Outcome']], size = decode_bin(['Byte'], msg['raw'][offset:])
    offset += size

    # Generate dictionary of all settings
    msg['Settings'] = []
    if msg['Outcome'] == 0x01:
        [msg['DeviceType'], msg['MajorVersion'], msg['MinorVersion'], msg['MoreSettings']], size = decode_bin(['UInt2', 'Byte', 'Byte', 'Byte'], msg['raw'][offset:])
        offset += size

        while offset < len(msg['raw']):
            # Get setting ID
            [SettingId], size = decode_bin(['UInt2'], msg['raw'][offset:])
            offset += size

            # Get flags and length
            [bit16], size = decode_bin(['UInt2'], msg['raw'][offset:])
            LargeValue = (bit16 & 0x8000) >> 15
            ReadOnly = (bit16 & 0x4000) >> 14
            SettingLen = bit16 & 0x3FFF
            offset += size

            # Get value
            SettingValue = msg['raw'][offset:offset+SettingLen]
            offset += SettingLen

            msg['Settings'].append({'SettingId': SettingId, 'SettingValue': SettingValue, 'LargeValue': LargeValue, 'ReadOnly': ReadOnly })

    return msg


################################################################################
#
# [1] section 2.2.6.2 DevConfig Set Settings Message (MsgType 0x10 & 0x90)
#
################################################################################

#
# Create DevConfig Set Settings Command packet
#
def pkt_devconfig_set_settings_cmd(DstNodeId, SrcNodeId, Settings = [], SecurityCode = 0x0000):
    # DstNodeId:        Destination node ID (12-bit int)
    # SrcNodeId:        Source node ID (12-bit int)
    # Settings:         List of dictionarys with SettingId and SettingValue fields for each setting (like 'Settings' returned by msg_devconfig_get_settings_response()
    # SecurityCode:     16-bit security code (optional)

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x0) # PakBus Control Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2'], [0x10, TranNbr, SecurityCode])

    # Encode setting Id, length and value
    for setting in Settings:
        if setting.has_key('SettingId') and setting.has_key('SettingValue'):
            msg += encode_bin(['UInt2', 'UInt2', 'ASCII'], [setting['SettingId'], len(setting['SettingValue']), setting['SettingValue']])

    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode DevConfig Set Settings Response packet
#
def msg_devconfig_set_settings_response(msg):
    # msg: decoded default message - must contain msg['raw']

    offset = 2
    [msg['Outcome']], size = decode_bin(['Byte'], msg['raw'][offset:])
    offset += size

    # Generate dictionary of all settings
    msg['SettingStatus'] = []
    if msg['Outcome'] == 0x01:
        while offset < len(msg['raw']):
            # Get setting ID
            [SettingId, SettingOutcome], size = decode_bin(['UInt2', 'Byte'], msg['raw'][offset:])
            offset += size

            msg['SettingStatus'].append({'SettingId': SettingId, 'SettingOutcome': SettingOutcome})

    return msg


################################################################################
#
# [1] section 2.2.6.3 DevConfig Get Setting Fragment Transaction Message (MsgType 0x11 & 0x91)
#
################################################################################

#
# still missing ...
#


################################################################################
#
# [1] section 2.2.6.4 DevConfig Set Setting Fragment Transaction Message (MsgType 0x12 & 0x92)
#
################################################################################

#
# still missing ...
#


################################################################################
#
# [1] section 2.2.6.4 DevConfig Control Transaction Message (MsgType 0x13 & 0x93)
#
################################################################################

#
# Create DevConfig Control Command packet
#
def pkt_devconfig_control_cmd(DstNodeId, SrcNodeId, Action = 0x04, SecurityCode = 0x0000):
    # DstNodeId:        Destination node ID (12-bit int)
    # SrcNodeId:        Source node ID (12-bit int)
    # Action:           The action that should be taken by the data logger (default: refresh session timer)
    # SecurityCode:     16-bit security code (optional)

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x0) # PakBus Control Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'Byte'], [0x13, TranNbr, SecurityCode, Action])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode DevConfig Control Response packet
#
def msg_devconfig_control_response(msg):
    # msg: decoded default message - must contain msg['raw']

    offset = 2
    [msg['Outcome']], size = decode_bin(['Byte'], msg['raw'][offset:])
    return msg


################################################################################
#
# [1] section 2.3 BMP5 Application Packets
#
################################################################################

################################################################################
#
# [1] section 2.3.1 Please Wait Message (MsgType 0xa1)
#
################################################################################

#
# Create Please Wait Message packet
#

#
# still missing ...
#

#
# Decode Please Wait Message packet
#
def msg_pleasewait(msg):
    # msg: decoded default message - must contain msg['raw']
    [msg['CmdMsgType'], msg['WaitSec']], size = decode_bin(['Byte', 'UInt2'], msg['raw'][2:])
    return msg


################################################################################
#
# [1] section 2.3.2 Clock Transaction (MsgType 0x17 & 0x97)
#
################################################################################

#
# Create Clock Command packet
#
def pkt_clock_cmd(DstNodeId, SrcNodeId, Adjustment = (0, 0), SecurityCode = 0x0000):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # Adjustment:   Clock adjustment (seconds, nanoseconds)
    # SecurityCode: 16-bit security code (optional)

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'NSec'], [0x17, TranNbr, SecurityCode, Adjustment])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode Clock Response packet
#
def msg_clock_response(msg):
    # msg: decoded default message - must contain msg['raw']
    [msg['RespCode'], msg['Time']], size = decode_bin(['Byte', 'NSec'], msg['raw'][2:])
    return msg


################################################################################
#
# [1] section 2.3.3.1 File Download Transaction (MsgType 0x1c & 0x9c)
#
################################################################################

#
# Create File Download Command packet
#
def pkt_filedownload_cmd(DstNodeId, SrcNodeId, FileName, FileData, SecurityCode = 0x0000, FileOffset = 0x00000000, TranNbr = None, CloseFlag = 0x01, Attribute = 0x00):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # FileName:     File name as string
    # FileData:     Binary string containing file data
    # SecurityCode: 16-bit security code (optional)
    # FileOffset:   Byte offset into the file or fragment
    # TranNbr:      Transaction number for continuig partial reads (required by OS>=17!)
    # CloseFlag:    Flag if file should be closed after this transaction
    # Attribute:    Reserved byte = 0x00

    # Generate new transaction number if none was supplied
    if not TranNbr:
        TranNbr = newTranNbr()
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'ASCIIZ', 'Byte', 'Byte', 'UInt4', 'ASCII'], [0x1c, TranNbr, SecurityCode, FileName, Attribute, CloseFlag, FileOffset, FileData])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode File Download Response packet
#
def msg_filedownload_response(msg):
    # msg: decoded default message - must contain msg['raw']

    [msg['RespCode'], msg['FileOffset']], size = decode_bin(['Byte', 'UInt4'], msg['raw'][2:])
    return msg


################################################################################
#
# [1] section 2.3.3.2 File Upload Transaction (MsgType 0x1d & 0x9d)
#
################################################################################

#
# Create File Upload Command packet
#
def pkt_fileupload_cmd(DstNodeId, SrcNodeId, FileName, SecurityCode = 0x0000, FileOffset = 0x00000000, TranNbr = None, CloseFlag = 0x01, Swath = 0x0200):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # FileName:     File name as string
    # SecurityCode: 16-bit security code (optional)
    # FileOffset:   Byte offset into the file or fragment
    # TranNbr:      Transaction number for continuig partial reads (required by OS>=17!)
    # CloseFlag:    Flag if file should be closed after this transaction
    # Swath:        Number of bytes to read

    # Generate new transaction number if none was supplied
    if not TranNbr:
        TranNbr = newTranNbr()
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'ASCIIZ', 'Byte', 'UInt4', 'UInt2'], [0x1d, TranNbr, SecurityCode, FileName, CloseFlag, FileOffset, Swath])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode File Upload Response packet
#
def msg_fileupload_response(msg):
    # msg: decoded default message - must contain msg['raw']

    [msg['RespCode'], msg['FileOffset']], size = decode_bin(['Byte', 'UInt4'], msg['raw'][2:7])
    msg['FileData'] = msg['raw'][7:] # return raw file data for later parsing
    return msg


################################################################################
#
# [1] section 2.3.3.3 File Directory Format
#
################################################################################

#
# Parse File Directory Format
#
def parse_filedir(raw):
    # raw:      Raw coded data string containing directory output

    offset = 0  # offset into raw buffer
    fd = { 'files': [] }     # initialize file directory structure
    [fd['DirVersion']], size = decode_bin(['Byte'], raw[offset:])
    offset += size

    # Extract file entries
    while True:
        file = {} # file description
        [filename], size = decode_bin(['ASCIIZ'], raw[offset:])
        offset += size

        # end loop when file attribute list terminator reached
        if filename == '': break

        file['FileName'] = filename
        [file['FileSize'], file['LastUpdate']], size = decode_bin(['UInt4', 'ASCIIZ'], raw[offset:])
        offset += size

        # Read file attribute list
        file['Attribute'] = [] # initialize file attribute list (up to 12)
        for i in range(12):
            [attribute], size = decode_bin(['Byte'], raw[offset:])
            offset += size
            if attribute:
                file['Attribute'].append(attribute) # append file attribute to list
            else:
                break # End of attribute list reached

        fd['files'].append(file) # add file entry to list

    return fd


################################################################################
#
# [1] section 2.3.3.4 File Control Transaction (MsgType 0x1e & 0x9e)
#
################################################################################

#
# Create File Control Transaction packet
#
def pkt_filecontrol_cmd(DstNodeId, SrcNodeId, FileName, FileCmd, SecurityCode = 0x0000, TranNbr = None):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # FileName:     File name as string
    # FileCmd:      Code that specifies the command to perform with the file
    # SecurityCode: 16-bit security code (optional)
    # TranNbr:      Transaction number for continuig partial reads (required by OS>=17!)

    # Generate new transaction number if none was supplied
    if not TranNbr:
        TranNbr = newTranNbr()
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'ASCIIZ', 'Byte'], [0x1e, TranNbr, SecurityCode, FileName, FileCmd])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode File Control Transaction Response packet
#
def msg_filecontrol_response(msg):
    # msg: decoded default message - must contain msg['raw']

    [msg['RespCode'], msg['HoldOff']], size = decode_bin(['Byte', 'UInt2'], msg['raw'][2:])
    return msg


################################################################################
#
# [1] section 2.3.3.5 Get Programming Statistics Transaction (MsgType 0x18 & 0x98)
#
################################################################################

#
# Create Get Programming Statistics Transaction packet
#
def pkt_getprogstat_cmd(DstNodeId, SrcNodeId, SecurityCode = 0x0000, TranNbr = None):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # SecurityCode: 16-bit security code (optional)
    # TranNbr:      Transaction number for continuig partial reads (required by OS>=17!)

    # Generate new transaction number if none was supplied
    if not TranNbr:
        TranNbr = newTranNbr()
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2'], [0x18, TranNbr, SecurityCode])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode Get Programming Statistics Transaction Response packet
#
def msg_getprogstat_response(msg):
    # msg: decoded default message - must contain msg['raw']

    # Get response code
    [msg['RespCode']], size = decode_bin(['Byte'], msg['raw'][2:])

    # Get report data if RespCode == 0
    if msg['RespCode'] == 0:
        [msg['OSVer'], msg['OSSig'], msg['SerialNbr'], msg['PowUpProg'], msg['CompState'], msg['ProgName'], msg['ProgSig'], msg['CompTime'], msg['CompResult']], size = decode_bin(['ASCIIZ', 'UInt2', 'ASCIIZ', 'ASCIIZ', 'Byte', 'ASCIIZ', 'UInt2', 'NSec', 'ASCIIZ'], msg['raw'][3:])

    return msg


################################################################################
#
# [1] section 2.3.4 Data Collection and Table Control Transactions
#
################################################################################

################################################################################
#
# [1] section 2.3.4.2 Getting Table Definitions and Table Signatures
#
################################################################################

#
# Parse table definition
#
def parse_tabledef(raw):
    # raw:      Raw coded data string containing table definition(s)

    TableDef = []   # List of table definitions

    offset = 0  # offset into raw buffer
    FslVersion, size = decode_bin(['Byte'], raw[offset:])
    offset += size

    # Parse list of table definitions
    while offset < len(raw):

        tblhdr = {}     # table header
        tblfld = []     # table field definitions
        start = offset  # start of table definition

        # Extract table header data
        [tblhdr['TableName'], tblhdr['TableSize'], tblhdr['TimeType'], tblhdr['TblTimeInto'], tblhdr['TblInterval']], size = decode_bin(['ASCIIZ', 'UInt4', 'Byte', 'NSec', 'NSec'], raw[offset:])
        offset += size

        # Extract field definitions
        while True:
            fld = {}
            [fieldtype], size = decode_bin(['Byte'], raw[offset:])
            offset += size

            # end loop when field list terminator reached
            if fieldtype == 0: break

            # Extract bits from fieldtype
            fld['ReadOnly'] = fieldtype >> 7    # only Bit 7

            # Convert fieldtype to ASCII FieldType (e.g. 'FP4') if possible, else return numerical value
            fld['FieldType'] = fieldtype & 0x7F # only Bits 0..6
            for Type in datatype.keys():
                if fld['FieldType'] == datatype[Type]['code']:
                    fld['FieldType'] = Type
                    break

            # Extract field name
            [fld['FieldName']], size = decode_bin(['ASCIIZ'], raw[offset:])
            offset += size

            # Extract AliasName list
            fld['AliasName'] = []
            while True:
                [aliasname], size = decode_bin(['ASCIIZ'], raw[offset:])
                offset += size
                if aliasname == '': break # Alias names list terminator reached
                fld['AliasName'].append(aliasname)

            # Extract other mandatory field definition items
            [fld['Processing'], fld['Units'], fld['Description'], fld['BegIdx'], fld['Dimension']], size = decode_bin(['ASCIIZ', 'ASCIIZ', 'ASCIIZ', 'UInt4', 'UInt4'], raw[offset:])
            offset += size

            # Extract sub dimension (if any)
            fld['SubDim'] = []
            while True:
                [subdim], size = decode_bin(['UInt4'], raw[offset:])
                offset += size
                if subdim == 0: break # sub-dimension list terminator reached
                fld['SubDim'].append(subdim)

            # append current field definition to list
            tblfld.append(fld)

        # calculate table signature
        tblsig = calcSigFor(raw[start:offset])

        # Append header, field list and signature to table definition list
        TableDef.append({'Header': tblhdr, 'Fields': tblfld, 'Signature': tblsig})

    return TableDef


################################################################################
#
# [1] section 2.3.4.3 Collect Data Transaction (MsgType 0x09 & 0x89)
#
################################################################################

#
# Create Collect Data Command packet
#
def pkt_collectdata_cmd(DstNodeId, SrcNodeId, TableNbr, TableDefSig, FieldNbr = [], CollectMode = 0x05, P1 = 0, P2 = 0, SecurityCode = 0x0000):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # TableNbr:     Table number
    # TableDefSig:  Table defintion signature
    # FieldNbr:     list of field numbers (empty to collect all)
    # CollectMode:  Collection mode code (P1 and P2 will be used depending on value)
    # P1:           1st parameter used to specify what to collect (optional)
    # P2:           2nd parameter used to specify what to collect (optional)
    # SecurityCode: security code of the data logger
    #
    # Note: theoretically, several requests with different TableNbr, Fieldnbr etc. could be
    #       requested in a single collect data command packet. This was not implemented
    #       on purpose, as the decoding of the retrieved packet is not trivial

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'Byte'], [0x09, TranNbr, SecurityCode, CollectMode])

    # encode table number and signature
    msg += encode_bin(['UInt2', 'UInt2'], [TableNbr, TableDefSig])

    # add P1 and P2 according to CollectMode
    if (CollectMode == 0x04) | (CollectMode == 0x05): # only P1 used (type UInt4)
        msg += encode_bin(['UInt4'], [P1])
    elif (CollectMode == 0x06) | (CollectMode == 0x08): # P1 and P2 used (type UInt4)
        msg += encode_bin(['UInt4', 'UInt4'], [P1, P2])
    elif CollectMode == 0x07: # P1 and P2 used (type NSec)
        msg += encode_bin(['NSec', 'NSec'], [P1, P2])

    # add field list
    fieldlist = FieldNbr + [0]
    msg += encode_bin(len(fieldlist) * ['UInt2'], fieldlist)

    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode Collect Data Response body
#
def msg_collectdata_response(msg):
    # msg: decoded default message - must contain msg['raw']

    offset = 2
    [msg['RespCode']], size = decode_bin(['Byte'], msg['raw'][offset:])
    offset += size

    msg['RecData'] = msg['raw'][offset:] # return raw record data for later parsing

    return msg

#
# Parse data returned by msg_collectdata_response(msg)
#
def parse_collectdata(raw, tabledef, FieldNbr = []):
    # raw:      Raw coded data string containing record data
    # tabledef: Table definition structure (as returned by parse_tabledef())
    # FieldNbr:     list of field numbers (empty to collect all)

    offset = 0
    recdata = [] # output structure

    while offset < len(raw) - 1:
        frag = {} # record fragment

        [frag['TableNbr'], frag['BegRecNbr']], size = decode_bin(['UInt2', 'UInt4'], raw[offset:])
        offset += size

        # Provide table name
        frag['TableName'] = tabledef[frag['TableNbr'] - 1]['Header']['TableName']

        # Decode number of records (16 bits) or ByteOffset (32 Bits)
        [isoffset], size = decode_bin(['Byte'], raw[offset:])
        frag['IsOffset'] = isoffset >> 7

        # Handle fragmented records (must be put together by external function)
        if frag['IsOffset']:
            [byteoffset], size = decode_bin(['UInt4'], raw[offset:])
            offset += size
            frag['ByteOffset'] = byteoffset & 0x7FFFFFFF
            frag['NbrOfRecs'] = None
            # Copy remaining raw data into RecFrag
            frag['RecFrag'] = raw[offset:-1]
            offset += len(frag['RecFrag'])

        # Handle complete records (standard case)
        else:
            [nbrofrecs], size = decode_bin(['UInt2'], raw[offset:])
            offset += size
            frag['NbrOfRecs'] = nbrofrecs & 0x7FFF
            frag['ByteOffset'] = None

            # Get time of first record and time interval information
            interval = tabledef[frag['TableNbr'] - 1]['Header']['TblInterval']
            if interval == (0, 0):  # event-driven table
                timeofrec = None
            else:                   # interval data, read time of first record
                [timeofrec], size = decode_bin(['NSec'], raw[offset:])
                offset += size

            # Loop over all records
            frag['RecFrag'] = []
            for n in range(frag['NbrOfRecs']):
                record = {}

                # Calculate current record number
                record['RecNbr'] = frag['BegRecNbr'] + n

                # Get TimeOfRec for interval data or event-driven tables
                if timeofrec:   # interval data
                    record['TimeOfRec'] = (timeofrec[0] + n * interval[0], timeofrec[1] + n * interval[1])
                else:           # event-driven, time data precedes each record
                    [record['TimeOfRec']], size = decode_bin(['NSec'], raw[offset:])
                    offset += size

                # Loop over all field indices
                record['Fields'] = {}
                if FieldNbr:    # explicit field numbers provided
                    fields = FieldNbr
                else:           # default: generate list of all fields in table
                    fields = range(1, len(tabledef[frag['TableNbr'] - 1]['Fields']) + 1)

                for field in fields:
                    fieldname = tabledef[frag['TableNbr'] - 1]['Fields'][field - 1]['FieldName']
                    fieldtype = tabledef[frag['TableNbr'] - 1]['Fields'][field - 1]['FieldType']
                    dimension = tabledef[frag['TableNbr'] - 1]['Fields'][field - 1]['Dimension']
                    if fieldtype == 'ASCII':
                        record['Fields'][fieldname], size = decode_bin([fieldtype], raw[offset:], dimension)
                    else:
                        record['Fields'][fieldname], size = decode_bin(dimension * [fieldtype], raw[offset:])
                    offset += size
                frag['RecFrag'].append(record)

        recdata.append(frag)

    # Get flag if more records exist
    [MoreRecsExist], size = decode_bin(['Bool'], raw[offset:])

    return recdata, MoreRecsExist


################################################################################
#
# [1] section 2.3.4.4 One-Way Data Transaction (MsgType 0x20 & 0x14)
#
################################################################################

#
# still missing ...
#


################################################################################
#
# [1] section 2.3.4.5 Table Control Transaction (MsgType 0x19 & 0x99)
#
################################################################################

#
# still missing ...
#


################################################################################
#
# [1] section 2.3.5 Get/Set Values Transaction (MsgType 0x1a, 0x9a, 0x1b, & 0x9b)
#
################################################################################

#
# Create Get Values Command packet
#
def pkt_getvalues_cmd(DstNodeId, SrcNodeId, TableName, Type, FieldName, Swath = 1, SecurityCode = 0x0000):
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # TableName:    Table name as string
    # Type:         Type name as defined in datatype (e.g. 'Byte')
    # FieldName:    Field name (including index if applicable)
    # Swath:        Number of columns to retrieve from an indexed field
    # SecurityCode: 16-bit security code (optional)

    TranNbr = newTranNbr()  # Generate new transaction number
    hdr = PakBus_hdr(DstNodeId, SrcNodeId, 0x1) # BMP5 Application Packet
    msg = encode_bin(['Byte', 'Byte', 'UInt2', 'ASCIIZ', 'Byte', 'ASCIIZ', 'UInt2'], [0x1a, TranNbr, SecurityCode, TableName, datatype[Type]['code'], FieldName, Swath])
    pkt = hdr + msg
    return pkt, TranNbr

#
# Decode Get Values Response packet
#
def msg_getvalues_response(msg):
    # msg: decoded default message - must contain msg['raw']
    [msg['RespCode']], size = decode_bin(['Byte'], msg['raw'][2])
    msg['Values'] = msg['raw'][3:] # return raw coded values for later parsing
    return msg

#
# Parse values retrieved from get values command
#
def parse_values(raw, Type, Swath = 1):
    # raw:      Raw coded data string containing values (as returned by decode_pkt)
    # Type:     Data type name as defined in datatype
    # Swath:    Number of columns to retrieve from an indexed field
    values, size = decode_bin(Swath * [Type], raw)
    return values

#
# Set Values Command Body (MsgType 0x1b)
#

#
# still missing ...
#


################################################################################
#
# Utility functions for encoding and decoding packets and messages
#
################################################################################

#
# Decode packet
#
def decode_pkt(pkt):
    # pkt: buffer containing unquoted packet, signature nullifier stripped

    # Initialize output variables
    hdr = {'LinkState': None, 'DstPhyAddr': None, 'ExpMoreCode': None, 'Priority': None, 'SrcPhyAddr': None, 'HiProtoCode': None, 'DstNodeId': None, 'HopCnt': None, 'SrcNodeId': None}
    msg = {'MsgType': None, 'TranNbr': None, 'raw': None}

    try:
        # decode PakBus header
        rawhdr = struct.unpack('>4H', pkt[0:8]) # raw header bits
        hdr['LinkState']   =  rawhdr[0] >> 12
        hdr['DstPhyAddr']  =  rawhdr[0] & 0x0FFF
        hdr['ExpMoreCode'] = (rawhdr[1] & 0xC000) >> 14
        hdr['Priority']    = (rawhdr[1] & 0x3000) >> 12
        hdr['SrcPhyAddr']  =  rawhdr[1] & 0x0FFF
        hdr['HiProtoCode'] =  rawhdr[2] >> 12
        hdr['DstNodeId']   =  rawhdr[2] & 0x0FFF
        hdr['HopCnt']      =  rawhdr[3] >> 12
        hdr['SrcNodeId']   =  rawhdr[3] & 0x0FFF

        # decode default message fields: raw message, message type and transaction number
        msg['raw'] = pkt[8:]
        [msg['MsgType'], msg['TranNbr']], size = decode_bin(('Byte', 'Byte'), msg['raw'][:2])
    except:
        pass

    # try to add fields from known message types
    try:
        msg = {
            # PakBus Control Packets
            (0, 0x09): msg_hello,
            (0, 0x89): msg_hello,
            (0, 0x8f): msg_devconfig_get_settings_response,
            (0, 0x90): msg_devconfig_set_settings_response,
            (0, 0x93): msg_devconfig_control_response,
           # BMP5 Application Packets
            (1, 0x89): msg_collectdata_response,
            (1, 0x97): msg_clock_response,
            (1, 0x98): msg_getprogstat_response,
            (1, 0x9a): msg_getvalues_response,
            (1, 0x9c): msg_filedownload_response,
            (1, 0x9d): msg_fileupload_response,
            (1, 0x9e): msg_filecontrol_response,
            (1, 0xa1): msg_pleasewait,
        }[(hdr['HiProtoCode'], msg['MsgType'])](msg)
    except KeyError:
        pass # if not listed above

    return hdr, msg

#
# Decode binary data according to data type
#
def decode_bin(Types, buff, length = 1):
    # Types:   List of strings containing data types for fields
    # buff:    Buffer containing binary data
    # length:  length of ASCII string (optional)

    offset = 0 # offset into buffer
    values = [] # list of values to return
    for Type in Types:
        # get default format and size for Type
        fmt = datatype[Type]['fmt']
        size = datatype[Type]['size']

        if Type == 'ASCIIZ': # special handling: nul-terminated string
            nul = buff.find('\0', offset) # find first '\0' after offset
            value = buff[offset:nul] # return string without trailing '\0'
            size = len(value) + 1
        elif Type == 'ASCII': # special handling: fixed-length string
            size = length
            value = buff[offset:offset + size] # return fixed-length string
        elif Type == 'FP2': # special handling: FP2 floating point number
            fp2 = struct.unpack(fmt, buff[offset:offset+size])
            mant = fp2[0] & 0x1FFF    # mantissa is in bits 1-13
            exp  = fp2[0] >> 13 & 0x3 # exponent is in bits 14-15
            sign = fp2[0] >> 15       # sign is in bit 16
            value = ((-1)**sign * float(mant) / 10**exp, )
        else:                # default decoding scheme
            value = struct.unpack(fmt, buff[offset:offset+size])

        # un-tuple single values
        if len(value) == 1:
            value = value[0]

        values.append(value)
        offset += size

    # Return decoded values and current offset into buffer (size)
    return values, offset


#
# Encode binary data according to data type
#
def encode_bin(Types, Values):
    # Types:   List of strings containing data types for fields
    # Values:  List of values (must have same number of elements as Types)

    buff = '' # buffer for binary data
    for i in range(len(Types)):
        Type = Types[i]
        fmt = datatype[Type]['fmt'] # get default format for Type
        value = Values[i]

        if Type == 'ASCIIZ':   # special handling: nul-terminated string
            value += '\0' # Add nul to end of string
            enc = struct.pack('%d%s' % (len(value), fmt), value)
        elif Type == 'ASCII':   # special handling: fixed-length string
            enc = struct.pack('%d%s' % (len(value), fmt), value)
        elif Type == 'NSec':   # special handling: NSec time
            enc = struct.pack(fmt, value[0], value[1])
        else:                  # default encoding scheme
            enc = struct.pack(fmt, value)

        buff += enc
    return buff


################################################################################
#
# Time and clock functions
#
################################################################################

# base of the epoch for NSec values (1990-01-01 00:00:00)
import calendar
nsec_base = calendar.timegm((1990, 1, 1, 0, 0, 0))

# length in seconds of one NSec tick (second integer value of an NSec value)
# Note: this should normally be 1E-9 (nanoseconds). However, for several OS versions < 17, this is
# actually 1e-6 for reading (not setting!) the clock
nsec_tick = 1E-9

#
# Convert nsec value to timestamp
#
def nsec_to_time(nsec, epoch = nsec_base, tick = nsec_tick):
    # nsec:  NSec value

    # Calculate timestamp with fractional seconds
    timestamp = epoch + nsec[0] + nsec[1] * tick
    return timestamp


#
# Convert timestamp to nsec value
#
def time_to_nsec(timestamp, epoch = nsec_base, tick = nsec_tick):
    # timestamp: timestamp with fractional seconds
    # epoch: start of epoch for absolute time calculations (default: nsec_base)
    #        set to zero for time differences

    # separate fractional and integer part of timestamp
    import math
    [fp, ip] = math.modf(timestamp)

    # Calculate two integer values for NSec
    nsec = (int(ip - epoch), int(fp / tick))
    return nsec


#
# Synchronize data logger clock with local clock
#
def clock_sync(s, DstNodeId, SrcNodeId, SecurityCode = 0x0000, min_adjust = 0.1, max_adjust = 3, offset = 0):
    # s:            Socket object
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # SecurityCode: 16-bit security code (optional)
    # min_adjust:   Minimum time difference to adjust clock [seconds]
    # max_adjust    Maximum adjustment in one step [seconds]
    # offset:       Offset of data loger clock from UTC [seconds]

    import time
    td = []

    # Read clock 10 times
    for j in range(10):
        pkt, TranNbr = pkt_clock_cmd(DstNodeId, SrcNodeId)
        t1 = time.time() # timestamp directly before sending clock command
        send(s, pkt)
        reftime = time.time() # reference time (UTC)
        hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)
        t2 = time.time() # timestamp directly after receiving clock response

        # Calculate time difference
        if msg.has_key('Time'):
            logtime = nsec_to_time(msg['Time']) - offset # time reported from data logger (UTC)
            delay = (t2 - t1) / 2 # time estimated delay from communication protocol
            td.append(logtime - reftime + delay) # build delay-corrected list of time differences
        else:
            break

    # Calculate mean time difference tdiff
    if len(td) > 2:
        # Drop shortest and longest time difference
        td.sort()
        del td[0]
        del td[-1]

        # Calculate average time difference
        tdiff = 0
        for t in td:
            tdiff += t
        tdiff /= len(td)

        # Calculate adjustment
        if abs(tdiff) > min_adjust:
            # Adjust clock
            adjust = max(min(-tdiff, max_adjust), -max_adjust)
            pkt, TranNbr = pkt_clock_cmd(DstNodeId, SrcNodeId, time_to_nsec(adjust, epoch = 0))
            send(s, pkt)
            hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)
        else:
            adjust = 0

    # could not calculate tdiff
    else:
        tdiff = None
        adjust = 0

    # return time difference (delay- and offset-corrected) and last adjustment
    return tdiff, adjust


################################################################################
#
# Utility functions for routine tasks
#
################################################################################

#
# Wait for an incoming packet
#
def wait_pkt(s, SrcNodeId, DstNodeId, TranNbr, timeout = 5):
    # s:            socket object
    # SrcNodeId:    source node ID (12-bit int)
    # DstNodeId:    destination node ID (12-bit int)
    # TranNbr:      expected transaction number
    # timeout:      timeout in seconds

    import time, socket
    max_time = time.time() + 0.9 * timeout

    # remember current timeout setting
    s_timeout = s.gettimeout()

    # Loop until timeout is reached
    while time.time() < max_time:
        s.settimeout(timeout)
        try:
            rcv = recv(s)
        except socket.timeout:
            rcv = ''
        hdr, msg = decode_pkt(rcv)

        # ignore packets that are not for us
        if hdr['DstNodeId'] != DstNodeId or hdr['SrcNodeId'] != SrcNodeId:
            continue

        # Respond to incoming hello command packets
        if msg['MsgType'] == 0x09:
            pkt = pkt_hello_response(hdr['SrcNodeId'], hdr['DstNodeId'], msg['TranNbr'])
            send(s, pkt)
            continue

        # Handle "please wait" packets
        if msg['TranNbr'] == TranNbr and msg['MsgType'] == 0xa1:
            timeout = msg['WaitSec']
            max_time += timeout
            continue

        # this should be the packet we are waiting for
        if msg['TranNbr'] == TranNbr:
            break

    else:
        hdr = {}
        msg = {}

    # restore previous timeout setting
    s.settimeout(s_timeout)

    return hdr, msg


#
# Download a complete file
#
def filedownload(s, DstNodeId, SrcNodeId, FileName, FileData, SecurityCode = 0x0000, Swath = 0x0200):
    # s:            Socket object
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # FileName:     File name as string
    # FileData:     File data as a binary string
    # SecurityCode: 16-bit security code (optional)
    # Swath:        Number of bytes transferred in each packet

    # Initialize return values
    RespCode = 0x0e

    # Send file download command packets until whole FileData has been transferred
    FileOffset = 0x00000000
    TranNbr = None
    CloseFlag = 0x00
    while not CloseFlag:

        # Check if this is the last packet
        if FileOffset + Swath >= len(FileData):
            CloseFlag = 0x01

        # Download Swath bytes after FileOffset from FileData
        pkt, TranNbr = pkt_filedownload_cmd(DstNodeId, SrcNodeId, FileName, FileData[FileOffset:FileOffset+Swath], FileOffset = FileOffset, TranNbr = TranNbr, CloseFlag = CloseFlag)
        send(s, pkt)
        hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)

        try:
            RespCode = msg['RespCode']
            # End loop if response code <> 0
            if RespCode <> 0:
                break
            # Append file data
            FileOffset += Swath
        except KeyError:
            break

    return RespCode


#
# Upload a complete file
#
def fileupload(s, DstNodeId, SrcNodeId, FileName, SecurityCode = 0x0000):
    # s:            Socket object
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # FileName:     File name as string
    # SecurityCode: 16-bit security code (optional)

    # Initialize return values
    RespCode = 0x0e
    FileData = ''

    # Send file upload command packets until no more data is returned
    FileOffset = 0x00000000
    TranNbr = None
    while True:

        # Upload chunk from file starting at FileOffset
        pkt, TranNbr = pkt_fileupload_cmd(DstNodeId, SrcNodeId, FileName, FileOffset = FileOffset, TranNbr = TranNbr, CloseFlag = 0x00)
        send(s, pkt)
        hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)

        try:
            RespCode = msg['RespCode']
            # End loop if no more data is returned
            if not msg['FileData']:
                break
            # Append file data
            FileData += msg['FileData']
            FileOffset += len(msg['FileData'])
        except KeyError:
            break

    return FileData, RespCode


#
# Get field value from table
#
def getvalues(s, DstNodeId, SrcNodeId, TableName, Type, FieldName, Swath = 1, SecurityCode = 0x0000):
    # s:            Socket object
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # TableName:    Table name as string
    # Type:         Type name as defined in datatype (e.g. 'Byte')
    # FieldName:    Field name (including index if applicable)
    # Swath:        Number of columns to retrieve from an indexed field
    # SecurityCode: 16-bit security code (optional)

    # Send Get Values Command and wait for repsonse
    try:
        pkt, TranNbr = pkt_getvalues_cmd(DstNodeId, SrcNodeId, TableName, Type, FieldName, Swath, SecurityCode)
        send(s, pkt)
        hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)
        values = msg_getvalues_response(msg)['Values']
        parse = parse_values(values, Type)
    except:
        parse = [ None ]

    # Return list with retrieved values
    return parse


#
# Collect data
#
def collect_data(s, DstNodeId, SrcNodeId, TableDef, TableName, FieldNames = [], CollectMode = 0x05, P1 = 1, P2 = 0, SecurityCode = 0x0000):
    # s:            Socket object
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)
    # TableDef:     Table definition structure (as returned by parse_tabledef())
    # TableName:    Table name as string
    # FieldName:    List of field names (empty to collect all), order does not matter
    # CollectMode:  Collection mode code (P1 and P2 will be used depending on value)
    # P1:           1st parameter used to specify what to collect (optional)
    # P2:           2nd parameter used to specify what to collect (optional)
    # SecurityCode: security code of the data logger

    # Get table number
    tablenbr = get_TableNbr(TableDef, TableName)
    if tablenbr is None:
        raise StandardError('table %s not found in table definition' % TableName)

    # Get table definition signature
    tabledefsig = TableDef[tablenbr - 1]['Signature']

    # Convert field names to list of field numbers
    fieldnames = FieldNames
    fieldnbr = []
    for fn in range(1, len(TableDef[tablenbr-1]['Fields']) + 1):
        fieldname = TableDef[tablenbr - 1]['Fields'][fn - 1]['FieldName']
        try:
            idx = fieldnames.index(fieldname)
        except ValueError:
            pass
        else:
            # Add field number to list and remove field name from search list
            fieldnbr.append(fn)
            del fieldnames[idx]
        # End loop if field name list is empty
        if not fieldnames:
            break
    # Issue warning if field names could not be resolved
    if fieldnames:
        raise Warning('field names not resolved for table %s: %s' % (TableName, fieldnames))

    # Send collect data request
    pkt, TranNbr = pkt_collectdata_cmd(DstNodeId, SrcNodeId, tablenbr, tabledefsig, FieldNbr = fieldnbr, CollectMode = CollectMode, P1 = P1, P2 = P2, SecurityCode = SecurityCode)
    send(s, pkt)
    hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)
    RecData, MoreRecsExist = parse_collectdata(msg['RecData'], TableDef, FieldNbr = fieldnbr)

    # Return parsed record data and flag if more records exist
    return RecData, MoreRecsExist


#
# Get table number from table name
#
def get_TableNbr(tabledef, TableName):
    # tabledef:  table definition structure (as returned by parse_tabledef)
    # TableName: table name

    TableNbr = None
    try:
        for i in range(len(tabledef)):
            if tabledef[i]['Header']['TableName'] == TableName:
                TableNbr = i + 1
                break
    except:
        pass

    return TableNbr


################################################################################
#
# Network utilities
#
################################################################################

#
# Open a socket to the PakBus port on a remote host
#
def open_socket(Host, Port = 6785, Timeout = 30):
    # Host:     Remote host IP address or name
    # Port:     TCP/IP port (defaults to 6785)
    # Timeout:  Socket timeout

    import socket
    for res in socket.getaddrinfo(Host, Port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        try:
            s = socket.socket(af, socktype, proto)
        except socket.error, msg:
            s = None
            continue
        try:
            # Set timeout and try to connect to socket
            s.settimeout(Timeout)
            s.connect(sa)
        except KeyError:
            s.close()
            s = None
            continue
        break

    # Return socket object or None
    return s


#
# Check if remote host is available
#
def ping_node(s, DstNodeId, SrcNodeId):
    # s:            Socket object
    # DstNodeId:    Destination node ID (12-bit int)
    # SrcNodeId:    Source node ID (12-bit int)

    # send hello command and wait for response packet
    pkt, TranNbr = pkt_hello_cmd(DstNodeId, SrcNodeId)
    send(s, pkt)
    hdr, msg = wait_pkt(s, DstNodeId, SrcNodeId, TranNbr)

    return msg