# node-mdb

**Note: This repository has been deprecated.  It has been left here for reference for anyone interested in viewing its logic etc.  Some links in the documentation may no longer work.  Please contact the author if you require further information.**
 
*node-mdb* is a Node.js-based Open Source clone of Amazon SimpleDB

It is a re-implementation of M/DB, but re-written in Node.js Javascript.  Like M/DB, it uses the free, Open Source 
GT.M database as the data repository.  *node-mdb* is a fully-fledged database, not simply a mock service.

Rob Tweed <rtweed@mgateway.com>  
23 June 2011
Copyright 2023: MGateway Ltd [https://www.mgateway.com](https://www.mgateway.com)  

Twitter @rtweed

Google Group for discussions, support, advice etc: [http://groups.google.co.uk/group/mdb-community-forum](http://groups.google.co.uk/group/mdb-community-forum)

## Important Note

All the SimpleDB APIs have been implemented, ie:

- BatchDeleteAttributes
- BatchPutAttributes
- CreateDomain
- DeleteDomain
- ListDomains
- DomainMetadata
- PutAttributes
- GetAttributes
- DeleteAttributes

However, none of these APIs currently support the NextToken mechanism, so all matching records will be returned.  You may find 
other missing functionality, but the most common use cases of the APIs should be fully compatible with SimpleDB.

Select can be used, but the only expression that has been implemented in Javascript within node-mdb at present is:

      Select * from [yourDomainName]

However, provided you are also running the legacy version of M/DB (ie the MDB.m routine should be
present), all other Select expressions will be handled by the legacy version, in which case
node-mdb will provide full Select functionality as per SimpleDB.
	  
Future versions will extend the capabilities of the Javascript implementation of the Select API.

Please report any problems to the M/DB Google Group: [http://groups.google.co.uk/group/mdb-community-forum](http://groups.google.co.uk/group/mdb-community-forum)

Note: the current version uses HTTP, but could be adapted for use with HTTPS
	  
## License

Copyright (c) 2023 MGateway Ltd,
Redhill, Surrey UK.
All rights reserved.

https://www.mgateway.com
Email: rtweed@mgateway.com

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.

## Running node-mdb

 There are two ways to run node-mdb:
 
 - using a pre-built, pre-configured virtual appliance
 - building your own system
 
## The node-mdb Virtual Appliance

 This is the quickest and simplest way to get a node-mdb system up and running.  See the 
 detailed instructions at the [M/Gateway web site] (http://www.mgateway.com/node-mdb.html).

## Building your own node-mdb system

To build your own node-mdb system, you'll first need to install and configure the Free Open Source NoSQL GT.M database
 [http://fisglobal.com/Products/TechnologyPlatforms/GTM/index.htm](http://fisglobal.com/Products/TechnologyPlatforms/GTM/index.htm). 

The reasons why I've used GT.M for emulation of SimpleDB's functionality are best summed up in this paper: [http://www.mgateway.com/documents/universalNoSQL.pdf](https://www.mgateway.com/documents/universalNoSQL.pdf)

You can download GT.M from [http://sourceforge.net/projects/fis-gtm/](http://sourceforge.net/projects/fis-gtm/).  Installation instructions are provided in the download kit. The 
 simplest way to install GT.M is to use the new installer: [http://sourceforge.net/projects/fis-gtm/files/GT.M%20Installer/v0.11/gtminstall] 
 (http://sourceforge.net/projects/fis-gtm/files/GT.M%20Installer/v0.11/gtminstall).

However, probably the quickest and easiest way to get a GT.M system going is to use Mike Clayton's *M/DB installer* for Ubuntu Linux which will create you a fully-working environment within a few minutes.  Mike's installer actually installs the previous version of M/DB as well as GT.M, but the instructions later in this ReadMe document explain how to upgrade to the new Node.js-based version.

Node.js can reside on the same server as GT.M or on a different server.

The instructions below assume you'll be installing Node.js and GT.M on the same server.

You can apply Mike's installer to a Ubuntu Linux system running on your own hardware, or running as a virtual machine.  However, I find Amazon EC2 servers to be ideal for trying this kind of stuff out.  I've tested it with Ubuntu 10.10.

So, for example, to create an M/DB Appliance using Amazon EC2:

- Start up a Ubuntu Lucid (10.10) instance, eg use ami-508c7839 for a 32-bit server version, or ami-548c783d for a 64-bit server version.

**32-bit Ubuntu:**

- Log in to your Ubuntu system and start a terminal session. If you've started a Ubuntu 10.4 or 10.10 EC2 AMI, log in with the username *ubuntu*

        sudo apt-get update
        cd /tmp
        wget http://michaelgclayton.s3.amazonaws.com/mgwtools/mgwtools-1.11_i386.deb
        sudo dpkg -i mgwtools-1.11_i386.deb (Ignore the errors that will be reported)
        sudo apt-get -f install (and type y when asked)
        rm mgwtools-1.11_i386.deb
	 
	 
**64-bit Ubuntu:**

- Log in to your Ubuntu system and start a terminal session. If you've started a Ubuntu 10.4 or 10.10 EC2 AMI, log in with the username *ubuntu*

        sudo apt-get update
        cd /tmp
        wget http://michaelgclayton.s3.amazonaws.com/mgwtools/mgwtools-1.11_amd64.deb
        sudo dpkg -i mgwtools-1.11_amd64.deb (Ignore the errors that will be reported)
        sudo apt-get -f install (and type y when asked)
        rm mgwtools-1.11_amd64.deb

If you point a browser at the domain name/IP address assigned to the Ubuntu machine, you should now get the M/DB welcome screen.  Follow the instructions you'll see in the welcome screen to initialise M/DB.

You now have a fully-working and configured GT.M database, ready for use with node-mdb.  Now follow the instructions below to upgrade to the new Node.js-based version of M/DB.

## Installing Node.js

Install Node.js and NPM on your server.  See [http://nodejs.org](http://nodejs.org) for details.


## Installing the node-mdb components

- download the node-mdb repository, eg

        git clone git://github.com/robtweed/node-mdb.git

 The destination directory in which you'll find the files is determined by the path in which you ran the above command.

 In the repository's */lib* directory, you'll find the main M/DB Node.js file: *mdb.js* and a second file named *mdbInit.js*.  Copy these files to the path */usr/local/gtm/ewd/* on your GT.M server (adjust this path appropriately if you've installed GT.M for use in another directory).

 If you've used Mike Clayton's M/DB Installer, you can skip the next steps in this section.
 
 If not, then in the repository's */gtm* directory, you'll find the M/Wire interface files that are used by mdb.js to access the GT.M database (See [https://github.com/robtweed/node-mwire](https://github.com/robtweed/node-mwire) for more details on the *node-mwire* used by *node-mdb*)  

You need to copy the files to the following directories:

  - Copy the file *mwire* to */etc/xinetd.d/mwire*
  - Copy the file *zmwire* to */usr/local/gtm/zmwire* and change its permissions to executable (eg 755)
  - Copy the file *zmwire.m* to */usr/local/gtm/ewd/zmwire.m*
  
 Next, you need to set up the port that is used by M/Wire to access GT.M.  Edit */etc/services* and add the line:
 
       mwire  6330/tcp  # Service for M/Wire Protocol
 
Nearly there!  You just need to use NPM to install some other Node.js modules that are used by node-mdb.

## Installing the additional Node.js modules used by node-mdb

Simply use NPM as follows:

        npm install node-mwire
		npm install redis-node
		npm install node-uuid
		
You're now ready to use *node-mdb*

## Setting up the administrator's security credentials

Before you run *node-mdb* for the first time, you need to set up an administrator's access key id and secret key.  These are used for signing the API requests (just as happens in SimpleDB).

If you used Mike Clayton's M/DB installer, you can do this via the welcome web page that you'll see in the browser when you first point it at the M/DB Appliance.  If you haven't used this process, you can now set up the security credentials using Node.js as follows:

Edit the file *mdbInit.js* to create the values for your administrator's access key id and secret key - these are clearly indicated at the top of the file (*It's not advisable to use the pre-defined values, so make sure you change them to something only you know*).  Then simply run it:

       cd /usr/local/gtm/ewd
	   node mdbInit.js
	   
You only have to run this once (or any time you want to change your administrator's security credentials).

## Running node-mdb

Now you're ready to fire up *node-mdb*:
	   
      cd /usr/local/gtm/ewd
	  node mdb.js
	  
That's it! It's now waiting for requests.
	  
Most SimpleDB clients will work with *node-mdb*.  The main requirement is that you must be able to change the URL that normally points to the Amazon SimpleDB server, and point it at your M/DB instance instead, eg:

      http://192.168.1.100:8081/

Clients that are known to work with M/DB include Bolso, boto (Python), Mindscape's SimpleDB Management Tools and Lightspeed.  A number of people have been able to simply modify the AWS Java client to work with M/DB, and the Node.js client (https://github.com/rjrodger/simpledb) should also work with *node-mdb*.
	  
By default, node-mdb listens on port 8081 for incoming HTTP requests.  You can change this and a number of other settings by editing *mdb.js*.  You'll find the customisable parameters described in the comments at the top of the file.

If you find any problems, please let me know via the M/DB Google Group: [http://groups.google.co.uk/group/mdb-community-forum](http://groups.google.co.uk/group/mdb-community-forum)
	  
 
