#!/usr/bin/python

# 1) Select the config file , default config given
# 2) get the required params
# 3) fix perms
# 4) Default: Interactive mode to prompt at each step

import sys
from optparse import OptionParser
import pwd
import grp
import os

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("-c", "--config-file", dest="configfile",
                  help="config file absolute location",
                  default="/etc/aerospike/aerospike.conf")
parser.add_option("-y", "--yes",
                  action="store_false", dest="interactive",
                  default=True,
                  help="Fix all permissions assuming" +
                  " default yes for all questions")

(options, args) = parser.parse_args()
# These are the params we are looking in config file
# to change ownership, add any new param to this dict
params = {'user': '',
          'group': '',
          'work-directory': '',
          'pidfile': '',
          'file': '',
          'user-path': '',
          'device': '',
          'namedpipe-path': '',
          'digestlog-path': '',
          'errorlog-path': '',
          'xdr-pidfile': ''
          }
# Default values of some of the params which need to be upgraded if no
# config values are present
params_def = {'work-directory': '/opt/aerospike/smd',
              'user-path': '/opt/aerospike/usr'
              }
# this is the dictionary for not running default chown
# devices dont persist ownership change through reboots.
# Hence adding it to exception list
# instead adding user to the group owning the device
# Also using this exclusion list for not adding parent dir
params_exc = {'device': '',
              'user': '',
              'group': ''
              }
param_names = params.keys()
u_configfile = ''

# check that the running user is root / sudo
if not os.getegid() == 0:
    print "Please run this script as root or with sudo"
    exit(10)

# Run in interactive mode by default , get config file absolute path
if options.interactive is True:
    u_configfile = raw_input("Enter config file (" +
                             options.configfile + "): ")
if not u_configfile:
    u_configfile = options.configfile
try:
    with open(u_configfile, 'r') as u_cf:
        for line in u_cf:
            # store the split
            l_line = line.split()
            # If line is not empty
            if(len(l_line) > 0):
                # Get the first word which is the param name
                line_param = l_line[0]
                # and check if the param is in the dictionary above
                if line_param in param_names:
                    # if present, then add the value to dict
                    p_value = l_line[1]
                    if (len(params[line_param]) == 0):
                        # This is the first value for the param
                        params[line_param] = p_value
                    else:
                        # The param already has a value, append
                        params[line_param] = params[line_param] + \
                            "," + p_value
                    # add the parent directory to  dict
                    # This helps in changing the ownership of the
                    # parent directory as well
                    if not line_param in params_exc:
                            params[line_param] = params[line_param] + \
                                "," + os.path.dirname(p_value)
except IOError as e:
    print 'Error while trying to read config file, please check ' + \
        u_configfile + ': ' + e.strerror
    exit(1)

u_user = params['user']
u_group = params['group']
# If either user or group is root, exit
if u_user == 'root' or u_group == 'root':
    print "root user or root group found in config, not making any changes"
    exit(2)

# get the uid/gid needed for chown
# This also verifies that the user exists
try:
    uid = pwd.getpwnam(u_user).pw_uid
    gid = grp.getgrnam(u_group).gr_gid
except KeyError as e:
    print ("Error getting uid/gid of the user/group specified in the"
           "config file : " + str(e))
    exit(3)

d_group = ''
# If device is in config, get the group for the device
if params['device'] is not '':
    u_device = params['device']
    # We are assuming all devices are owned by the same group
    # hence finding group only for the first device
    if(',' in u_device):
            u_device = params['device'].split(',')[0]
    try:
        d_info = os.stat(u_device)
        d_gid = d_info.st_gid
        d_group = grp.getgrgid(d_gid)[0]
    except OSError as e:
        print e.strerror + " " + u_device
        exit(5)

# This is the default behavior for the script
chown_default = 'y'
do_chown = chown_default

usermod_default = chown_default
do_usermod = usermod_default
# add user to group of device , default interactive
if options.interactive is True and d_group is not '':
    do_usermod = raw_input("Add user " + u_group +
                           " to group " + d_group + "? (" + do_usermod + ")")
    if not do_usermod:
        do_usermod = usermod_default
cmd = "usermod -a -G " + d_group + " " + u_user
if do_usermod == 'y' and d_group is not '':
    try:
        if os.system(cmd) == 0:
            print "User " + u_user + " added to group disk"
        else:
            # The exit() call here will raise an exception (SystemExit)
            # So, let us not print any error here.
            # Let the exception handler do it
            exit(7)
    except:
        print "Error while adding user to group disk. "
        exit(6)


# removing the keys from dict which we dont need anymore
# This also helps to iterate the entire dict later for changing owner
params.pop('device')
params.pop('user')
params.pop('group')

#check for the default params list. If its not in config params, add it
for k in params_def:
    if not len(params[k]) > 0:
        params[k] = params_def[k]

# chown for all params
for k in params:
    # except for the params mentioned in exclusion list
    if k not in params_exc:
        param = params[k]
        if len(param) > 0:
            if(',' in param):
                # if param has multiple values, get them in a list
                n_params = param.split(',')
            else:
                n_params = [param]
            for n_param in n_params:
                # if the user chose interactive mode ask the question,
                # else assume its a 'yes'
                if options.interactive is True:
                    do_chown = raw_input("change ownership of " + n_param +
                                         " to user " + u_user +
                                         " group " + u_group +
                                         " (" + chown_default + "):")
                    if not do_chown:
                        do_chown = chown_default
                else:
                    do_chown = 'y'

                if do_chown == 'y':
                    try:
                        os.chown(n_param, uid, gid)
                        print ("Ownership changed of " + n_param +
                               " to user " + u_user +
                               " group " + u_group)
                    except OSError as e:
                        print "Error: " + n_param, e.strerror
                        #prompt for all except pid file
                        if 'pid' not in n_param and options.interactive is True:
                            do_continue = raw_input(n_param + " doesn't exist;"
                                                "Do you want to continue?" +
                                                "(" + chown_default + "):")
                            if not do_continue:
                                do_continue = chown_default
                            if not do_continue == 'y':
                                exit(4)

# The reason for deleting shared memory -
# We create the shared memory with 666 permission
# The non-root run can continue running with warm restart but
# cold restart will have problems deleting the shared memory
delshm_default = chown_default
do_delshm = delshm_default
if options.interactive is True:
    do_delshm = raw_input("Delete all shared memory instances " +
                          "used by aerospike server? You can check" +
                          " https://docs.aerospike.com" +
                          "/display/V3/Warm+Start for more details" +
                          "(" + delshm_default + "):")
    if not do_delshm:
        do_delshm = delshm_default
if do_delshm == 'y':
    cmd = "for i in `ipcs -m| sed \"s/ .*$//\" |grep 0xae`;do ipcrm -M $i;done"
    try:
        if os.system(cmd) == 0:
            print "Shared memory used by aerospike deleted"
        else:
            # The exit() call here will raise an exception (SystemExit)
            # So, let us not print any error here.
            # Let the exception handler do it
            exit(8)
    except:
        print "Error while deleting shared memory "
        exit(9)

#If we have come this far, there have been no exits, we should be good to print
#an INFO message saying all good. This message should be used as a debug param 
#for successful execution
print "INFO: Successful execution of fixownership script finished" 

