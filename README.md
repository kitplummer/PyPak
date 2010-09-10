License & Copyright
-------------------

(c) 2009 Dietrich Feist, Max Planck Institute for Biogeochemistry, Jena Germany

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.


Overview
--------

PyPak is a Python library for communication with Campbell Scientific data
loggers through the PakBus interface. It may be useful for people who would
prefer to use their own Python scripts to communicate with their data logger.

The software has been developed using Python 2.5 on Debian Linux and a CR1000
data logger with a NL115 ethernet interface and documentation available from
Campbell Scientific [1, 2]. In principle, it should also run on other platforms
and over a serial connection. However, this has not been tested. You should run
at least run OSVersion 17 on your data logger to make sure that some critical
bugs have been fixed.

Things that have been implemented:

- encoding and decoding of PakBus packets (PakCtrl & BMP5)
- reading and adjusting the data logger's internal clock
- file upload transactions
- file download transactions
- file control transactions
- retrieving table definitions
- retrieving table data
- reading and setting of DevConfig settings
- basic handling of DevConfig control messages

Things that are not yet implmemnted:

- DevConfig fragment handling
- one-way data transactions
- table control transactions


Installation
------------

- install Python on your system
- copy the file pakbus.py from the "python" folder into your Python search path


How to get started
------------------

Try the examples in the "examples" folder:

IMPORTANT: to run the examples, you have to adapt the host name/address (for TCP/IP
connections) and the PakBus IDs for your computer and data logger in the file 'pakbus.conf' first!

show_clock.py: shows the clock offset between your computer and your data logger

show_files.py: lists files on your data logger

show_tabledef.py: outputs the table structure from your data logger

show_progstat.py: outputs the compile status of the (running) program

All examples only read data from the logger and should not be able to destroy anything. However, you should not try them on a logger taking mission-critical data. Backing up your programs and data first is strongly recommended.

More sophisticated examples like CR1000 to MySQL data transfer are available from the
author on request.


How to contact the author
-------------------------

If you find the software useful, please let me know:

Dr. Dietrich Feist
Max Planck Institute for Biogeochemistry
Hans-Knoell-Str. 10
07745 Jena
Germany

Phone: +49-3641-57 63 78
Email: dfeist@bgc-jena.mpg.de


Trademark disclaimer
--------------------

Product names, logos, brands, and other trademarks featured or referred to
within the PyPak documentation or source code are the property of their
respective trademark holders. These trademark holders are not affiliated with
the author or the PyPak project in any way.


References
----------

[1] BMP5 Transparent Commands Manual, Rev. 9/08, Campbell Scientific Inc., 2008
[2] PakBus Networking Guide for the CR10X, CR510, CR23X, and CR200 Series
    and LoggerNet 2.1C, Rev. 3/05, Campbell Scientific Inc., 2004-2005
