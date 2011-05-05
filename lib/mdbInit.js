/*

mdbInit.js

 ----------------------------------------------------------------------------
 | Node.js version of M/DB: An emulation of SimpleDB                        |
 |                                                                          |
 | Copyright (c) 2011 M/Gateway Developments Ltd,                           |
 | Reigate, Surrey UK.                                                      |
 | All rights reserved.                                                     |
 |                                                                          |
 | http://www.mgateway.com                                                  |
 | Email: rtweed@mgateway.com                                               |
 |                                                                          |
 | This program is free software: you can redistribute it and/or modify     |
 | it under the terms of the GNU Affero General Public License as           |
 | published by the Free Software Foundation, either version 3 of the       |
 | License, or (at your option) any later version.                          |
 |                                                                          |
 | This program is distributed in the hope that it will be useful,          |
 | but WITHOUT ANY WARRANTY; without even the implied warranty of           |
 | MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            |
 | GNU Affero General Public License for more details.                      |
 |                                                                          |
 | You should have received a copy of the GNU Affero General Public License |
 | along with this program.  If not, see <http://www.gnu.org/licenses/>.    |
 ----------------------------------------------------------------------------

Initialises a new M/DB database with an administrator access key and secretkey

Edit the accessKeyId and secretKey values below as required

Adjust the M/Wire parameters if required

****************************************************************
*/

// Edit these to your own values!

var accessKeyId = 'administrator';
var secretKey = 'mdbsecretkey';

//-----------------------------------------

var database = 'gtm';
var mwire = {};
var db;
if (database === 'gtm') {
  var mwire = {
    port: 6330,
    host: '127.0.0.1',
    poolSize: 4
  };
  var mwireLib = require("node-mwire");
  db = new mwireLib.Client({port: mwire.port, host: mwire.host, poolSize: mwire.poolSize});
}

var count = 0;

var confirm = function(accessKeyId, secretKey) {
  console.log("'" + accessKeyId + "' has been set up as an administrator with a secret key = '" + secretKey + "'");
  process.exit(1);
};

db.clientPool[db.connection()].setGlobal('MDBUAF', ['administrator'], accessKeyId, function (error, results) { 
 count++;
 if (count === 2) confirm(accessKeyId, secretKey);
});
db.clientPool[db.connection()].setGlobal('MDBUAF', ['keys', accessKeyId], secretKey, function (error, results) { 
 count++;
 if (count === 2) confirm(accessKeyId, secretKey);
});








