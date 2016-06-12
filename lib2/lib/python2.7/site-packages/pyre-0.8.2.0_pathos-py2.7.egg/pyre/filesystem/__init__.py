#!/usr/bin/env python
# 
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 
#                               Michael A.G. Aivazis
#                        California Institute of Technology
#                        (C) 1998-2005 All Rights Reserved
# 
#  <LicenseText>
# 
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 

# factories

def root(name):
    from Root import Root
    return Root(name)


def directory(name, parent):
    from Directory import Directory
    return Directory(name, parent)


def file(name, parent):
    from File import File
    return File(name, parent)


# version
__id__ = "$Id: __init__.py,v 1.1.1.1 2005/03/08 16:13:46 aivazis Exp $"

#  End of file 
