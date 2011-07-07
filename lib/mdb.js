/*
                  ++++   node-mdb    ++++
                  ++++   Build 10   ++++
                  ++++  06 July 2011  ++++

 ----------------------------------------------------------------------------
 | node-mdb: Node.js version of M/DB                                        |
 |                                                                          |
 | M/DB: An emulation of SimpleDB                                           |
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

 Modify the parameters below if required:

 - httpPort: the port on which Node listens for incoming HTTP requests

 - trace: true|false - controls how much is reported to the console.log
                       If set false, just BoxUsage is reported for each request
 - sdbURLPattern: the URL location, as per standard SimpleDB requests

 - mdbURLPattern: for users of earlier, non-Node.js versions of M/DB, to ensure
                  compatibility with clients configured for M/DB

 - database: currently just GT.M, in which case ensure the M/Wire parameters
             are correctly defined for your GT.M environment.  The usual defaults
             are used below.

 - silentStart: turns off all logging to console.log

 - useLegacySelect: by setting this to true, node-mdb will pass Select queries to 
                    the native M coded Select logic within the legacy M/DB GT.M routine.
                    If set to true, you *must* have the legacy version of M/DB installed

****************************************************************

*/

var httpPort = 8081;
var trace = true;

var sdbURLPattern = '/';
var mdbURLPattern = '/mdb/request.mgwsi';

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
var silentStart = false;
var useLegacySelect = true;

/*
*****************    Set up environment     ********************
*/

var http = require("http");
var url = require("url");
var queryString = require("querystring");
var path = require("path"); 
var uuid = require("node-uuid");
var crypto = require("crypto");

var i;
var accessKey = {};
var MDB = {
  indexLength:180,
  getJSON: function(subscripts, callback) {
    db.clientPool[db.connection()].getJSON('MDB', subscripts, callback);
  },
  getGlobal: function(subscripts, callback) {
    db.clientPool[db.connection()].getGlobal('MDB', subscripts, callback);
  },
  kill: function(subscripts, callback) {
    db.clientPool[db.connection()].kill('MDB', subscripts, callback);
  },
  getAllSubscripts: function(subscripts, callback) {
    db.clientPool[db.connection()].getAllSubscripts('MDB', subscripts, callback);
  },
  increment: function(subscripts, delta, callback) {
    db.clientPool[db.connection()].increment('MDB', subscripts, delta, callback);
  },
  decrement: function(subscripts, delta, callback) {
    db.clientPool[db.connection()].decrement('MDB', subscripts, delta, callback);
  },
  setGlobal: function(subscripts, value, callback) {
    db.clientPool[db.connection()].setGlobal('MDB', subscripts, value, callback);
  },
  select: function(SDB, callback) {
    db.clientPool[db.connection()].remoteFunction('externalSelect^MDB', [SDB.nvps.AWSAccessKeyId, SDB.nvps.SelectExpression], callback);
  },
  batchPutItem: function(domainId, itemName, attributes, SDB, callback) {
    var attrsJSON = JSON.stringify(attributes);
    db.clientPool[db.connection()].remoteFunction('batchPutItem^MDB', [SDB.nvps.AWSAccessKeyId, domainId, itemName, attrsJSON], callback);
  }
};

/*
******************* Web Server *********************************
*/

http.createServer(function(request, response) {

  request.content = '';
  request.setEncoding("utf8");

  request.on("data", function(chunk) {
       request.content += chunk;
  });

  request.on("end", function(){
    //console.log("request = " + request.content);
    //console.log("headers: " + JSON.stringify(request.headers));
    var SDB = {startTime: new Date().getTime(),
               request: request,
               response: response
    };
    var urlObj = url.parse(request.url, true); 
    if (request.method === 'POST') {
      SDB.nvps = parseContent(request.content);
    }
    else {
      SDB.nvps = urlObj.query;
    }
    //console.log("nvps = " + JSON.stringify(nvps));
    //console.log("queryString: " + JSON.stringify(urlObj.query));
    var uri = urlObj.pathname;
    if ((uri.indexOf(sdbURLPattern) !== -1)||(uri.indexOf(mdbURLPattern) !== -1)) {
      //appears to be an SDB (M/DB) request
      processSDBRequest(SDB);
    }
    else { 
      //console.log("Bad M/DB URL Pattern");
      var uriString = 'http://' + request.headers.host + request.url;
      var error = {code:'InvalidURI', message: 'The URI ' + uriString + ' is not valid',status:400};
      returnError(SDB ,error);
    }
  });

}).listen(httpPort);

var processSDBRequest = function(SDB) {
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  // check security credentials are present and fetch secretKey if not already available
  if (!accessKeyId) {
    if (trace) console.log("missing Access Key Id");
    var error = {code:'AuthMissingFailure', message: 'AWS was not able to authenticate the request: access credentials are missing',status:403};
    returnError(SDB, error);
  }
  else {
    if (accessKey[accessKeyId]) {
       //console.log("secret key already locally held: " + accessKey[SDB.nvps.AWSAccessKeyId]);
       // Commence signature validation process
       validateSDBRequest(SDB, accessKey[accessKeyId]);
    }
    else {
      if (database === 'gtm') {
        db.clientPool[db.connection()].getGlobal('MDBUAF', ['keys', accessKeyId],
           function (error, results) {
            if (!error) {
              if (results.value !== '') {
                accessKey[accessKeyId] = results.value;
                if (trace) console.log("secret key fetched: " + results.value);
                // Commence signature validation process
                validateSDBRequest(SDB, results.value);
              }
              else {
                if (trace) console.log("missing Access Key Id");
                var error = {code:'AuthMissingFailure', message: 'AWS was not able to authenticate the request: access credentials are missing',status:403};
                returnError(SDB, error);
              }
            }
          }
        );
      }
    }
  }
};

/*
***********  Request Signature Security Validation ****************
*/

var validateSDBRequest = function(SDB, secretKey) {
  //check validity of request signature, based on security credentials
  var nvps = SDB.nvps;
  var type;
  var stringToSign = createStringToSign(SDB, true, "uri");
  if (stringToSign === -1) {
    if (trace) console.log("invalid signature version");
    var error = {code:'InvalidParameterValue', message: 'Value (' + nvps.SignatureVersion + ') for parameter SignatureVersion is invalid. Valid Signature versions are 0, 1 and 2',status:400};
    returnError(SDB, error);
  }
  else {
    if (trace) console.log("StringToSign = " + stringToSign);
    switch (nvps.SignatureMethod) {
       case 'HmacSHA1':
         type = 'sha1';
         break;
       case 'HmacSHA256':
         type = 'sha256';
         break;
       case 'HmacSHA512':
         type = 'sha512';
         break;
       default:
         type = 'sha1';
    }
    var hash = digest(stringToSign, secretKey, type);
    if (trace) console.log("hash = " + hash + "; Signature = " + nvps.Signature);
    if (hash === nvps.Signature) {
      processSDBAction(SDB);
    }
    else {
      // try again without including the port
      stringToSign = createStringToSign(SDB, false, "uri");
      hash = digest(stringToSign, secretKey, type);
      if (trace) console.log("2nd try without port: hash = " + hash + "; Signature = " + nvps.Signature);
      if (hash === nvps.Signature) {
        processSDBAction(SDB);
      }
      else {
        // try again using escape rather than encodeURIComponent
        stringToSign = createStringToSign(SDB, true, "escape");
        hash = digest(stringToSign, secretKey, type);
        if (trace) console.log("3rd try using standard escape: hash = " + hash + "; Signature = " + nvps.Signature);
        if (trace) console.log("String to sign: " + stringToSign);
        if (hash === nvps.Signature) {
          processSDBAction(SDB);
        }
        else {
          if (trace) console.log("Signatures don't match");
          errorResponse('SignatureDoesNotMatch', SDB)
        }
      }
    }
  }
};

/*
*********** Action Selection/Processing *********************
*/

var processSDBAction = function(SDB) {
  var nvps = SDB.nvps;
  var action = nvps.Action;
  if (!action) {
    if (trace) console.log("missing action");
    errorResponse('MissingAction', SDB)
  }
  else {

    if (!nvps.Version) SDB.nvps.Version = '2009-04-15';
    if (trace) console.log("Action = " + action);

    switch (action) {
      case "ListDomains":
        ListDomains(SDB);
        break;

      case "CreateDomain":
        CreateDomain(SDB);
        break;

      case "DeleteDomain":
        DeleteDomain(SDB);
        break;

      case "DomainMetadata":
        DomainMetadata(SDB);
        break;

      case "GetAttributes":
        GetAttributes(SDB);
        break;

      case "PutAttributes":
        PutAttributes(SDB);
        break;

      case "BatchPutAttributes":
        BatchPutAttributes(SDB);
        break;

      case "DeleteAttributes":
        DeleteAttributes(SDB);
        break;

      case "BatchDeleteAttributes":
        BatchDeleteAttributes(SDB);
        break;

      case "Select":
        Select(SDB);
        break;

      default:
        // error unrecognised command
        if (trace) console.log("unrecognised Action");
        var error = {code:'InvalidAction', message: 'The action ' + action + ' is not valid for this web service',status:400};
        returnError(SDB, error);
    }
  }
};

/*
 *****************  Actions  ***************************
*/

/*
 *****************  Select  ***************************

  ** Note! The Select implementation is incomplete at present.

  The only expressions that works are:

   Select * from [domainName]
   Select * from [domainName] Limit [n]

*/

var Select = function(SDB) {

  var error = {code:'InvalidQueryExpression', message: 'The specified query expression syntax is not valid.',status:400};
  var expression = SDB.nvps.SelectExpression;
  var condition = '';
  if (trace) console.log("Select Expression = " + expression);
  var piece = expression.split(' ');
  if (piece[0].toLowerCase() !== 'select') {
    returnError(SDB, error);
    return;
  }
  if (piece[1] !== '*') {
    //returnError(SDB, error);
    //return;
    condition = piece[1];
  }
  if (piece[2].toLowerCase() !== 'from') {
    //returnError(SDB, error);
    //return;
  }
  if (typeof piece[3] === 'undefined') {
    returnError(SDB, error);
    return;
  }
  var domainName = piece[3];
  if ((domainName.charAt(0) === '`')&&(domainName.charAt(domainName.length - 1) === '`')) {
    domainName = domainName.substring(1,domainName.length - 1);
    //console.log("1 domainName = " + domainName);
  }
  var limit = 0;

  if (typeof piece[4] !== 'undefined') {
    if (piece[4].toLowerCase() === 'limit') {
      limit = parseInt(piece[5]);
    }
    if (piece[4].toLowerCase() === 'where') {
      // not yet supported!
      //returnError(SDB, error);
      //return;
      condition = piece[5];
    }
  }
  SDB.nvps.DomainName = domainName;
  //condition = 1;
  SDB.select = {
    items: piece[1],
    condition: condition,
    limit: limit
  };
  if (trace) console.log("*** SDB.select = " + JSON.stringify(SDB.select));
  
  var invoke = {
       found: function(domainIndex, SDB) {
         select(domainIndex, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB);
};

var select = function(domainIndex, SDB) {
  SDB.select.domainIndex = domainIndex;
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  if (trace) console.log("in select - domainIndex = " + domainIndex);
  if (trace) console.log("SDB.select = " + JSON.stringify(SDB.select));
  if ((SDB.select.items === '*')&&(SDB.select.condition === '')) {
    SDB.select.xml = {};
    MDB.getAllSubscripts([accessKeyId, 'domains', domainIndex, 'items'], function (error, results) {
      if (trace) console.log("itemIndex array = " + JSON.stringify(results));
      SDB.select.items = results;
      if (results.length > 0) {
        // now get attribs Names
        MDB.getJSON([accessKeyId, 'domains', domainIndex, 'attribs'], function (error, results) {
          if (trace) console.log("attribs = " + JSON.stringify(results));
          SDB.select.attribs = results;
          SDB.select.count = 0;
          for (var i = 0; i < SDB.select.items.length; i++) {
            var itemIndex = SDB.select.items[i];
            getItemDetails(itemIndex, SDB);
          }
        });
      }
      else {
        SelectResponse(SDB);
      }
    });
  }
  else {
    if (useLegacySelect) {
      // Interim use of M/DB's native coded Select logic for anything
      // other than simple select * from domain queries
      if (SDB.nvps.SelectExpression.indexOf('"\\"') !== -1) {
        // replace double quoted backslash with a single quoted one to ensure correct transmission
        SDB.nvps.SelectExpression = SDB.nvps.SelectExpression.replace(/\x22\x5C\x22/g,'\x27\x5C\x27');
        //console.log("assss SDB.nvps.SelectExpression=" + SDB.nvps.SelectExpression);
      }
      MDB.select(SDB, function(error, results) {
        //console.log("select results: " + JSON.stringify(results));
        if (results.error) {
          var error = {code: results.error.errorCode, message: results.error.errorMessage, status:400};
          returnError(SDB, error);
        }
        else {
          var nvps = SDB.nvps;
          var action = nvps.Action;
          var item;
          var itemNo;
          var attrs;
          var attr;
          var attrNo;
          var attrName;
          var valueNo;
          var values;
          var xml;
          var noOfItems = 0;
          for (itemNo in results) noOfItems++;
          if (noOfItems === 0) {
            xml = responseStart({action: action, version: nvps.Version, hasContent: false, isEmpty: true});
          }
          else {
            xml = responseStart({action: action, version: nvps.Version, hasContent: true, isEmpty: false});
          }
          for (itemNo in results) {
            item = results[itemNo];
            xml = xml + '<Item><Name>' + item.i + '</Name>';
            attrs = item.a;
            for (attrNo in attrs) {
              attr = attrs[attrNo];
              attrName = attr.n;
              values = attr.v;
              for (valueNo in values) {
                value = values[valueNo] + '';
                value = value.replace(/\\\\/g, "\\")
                xml = xml + '<Attribute><Name>' + attrName + '</Name><Value>' + xmlEscape(value) + '</Value></Attribute>';
              }
            }
            xml = xml + '</Item>';
          }
          var hasContent = (noOfItems > 0);
          xml = xml + responseEnd(action, SDB.startTime, hasContent);
          writeResponse(200, xml, SDB.response);
        }
      });
    }
    else {
      var error = {code:'InvalidQueryExpression', message: 'The specified query expression syntax is not valid.',status:400};
      returnError(SDB, error);
    }
  }

};

var getItemDetails = function(itemIndex, SDB) {
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  var domainIndex = SDB.select.domainIndex;
  if (trace) console.log("get details for itemIndex = " + itemIndex);
  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', itemIndex], function (error, results) {
    if (trace) console.log("item contents = " + JSON.stringify(results));
    var attribs = results.attribs;
    var attribNames = SDB.select.attribs;
    var itemName = attribs['0'].value['1'];
    var attribIndex;
    var attrib;
    var values;
    var valueIndex;
    var attribName;
    var attribValue;
    var xml = '<Item><Name>' + itemName + '</Name>';
    for (attribIndex in attribs) {
      if (attribIndex !== '0') {
        attribName = attribNames[attribIndex];
        attrib = attribs[attribIndex];
        if (typeof attrib.value !== 'undefined') {
          values = attrib.value;
          for (valueIndex in values) {
            attribValue = xmlEscape(values[valueIndex]);
            xml = xml + '<Attribute><Name>' + attribName + '</Name><Value>' + attribValue + '</Value></Attribute>';
          }
        }
      }
    }
    xml = xml + '</Item>';
    SDB.select.xml[itemIndex] = xml;
    SDB.select.count++;
    if (SDB.select.count === SDB.select.items.length) {
      SelectResponse(SDB);
    }
  });
};

var SelectResponse = function(SDB) {
  var nvps = SDB.nvps;
  var action = nvps.Action;
  var selectXML = SDB.select.xml;
  var xml;
  var itemIndex;
  //console.log("xml = " + JSON.stringify(selectXML));
  if (SDB.select.items.length === 0) {
    xml = responseStart({action: action, version: nvps.Version, hasContent: false, isEmpty: true});
  }
  else {
    xml = responseStart({action: action, version: nvps.Version, hasContent: true, isEmpty: false});
  }
  for (var i = 0; i < SDB.select.items.length; i++) {
    itemIndex = SDB.select.items[i];
    xml = xml + selectXML[itemIndex];
  }
  var hasContent = (SDB.select.items.length > 0);
  xml = xml + responseEnd(action, SDB.startTime, hasContent);
  writeResponse(200, xml, SDB.response)
};

/*
 *****************  ListDomains  ***************************
*/

var ListDomains = function(SDB) {
  var nvps = SDB.nvps;
  var accessKeyId = nvps.AWSAccessKeyId;
  //db.clientPool[db.connection()].getJSON([accessKeyId, 'domainIndex'],
  //  function(err, json) {
  MDB.getJSON([accessKeyId, 'domainIndex'],
    function(err, json) {
      if (!err) {
        //console.log("json=" + JSON.stringify(json));
        var shortName;
        var value;
        var domainId;
        var noOfDomains = 0;
        for (shortName in json) {
          noOfDomains++;
        }
        var maxDomains = noOfDomains;
        if (nvps.MaxNumberOfDomains) {
          if (nvps.MaxNumberOfDomains < noOfDomains) maxDomains = nvps.MaxNumberOfDomains;
        }
        //console.log("fetching " + maxDomains + " domains from database");
        var domainList = [];
        if (noOfDomains === 0) {
          ListDomainsResponse(domainList, SDB);
        }
        else {
          var count = 0;
          for (shortName in json) {
            value = json[shortName];
            for (domainId in value) {
              MDB.getGlobal([accessKeyId, 'domains', domainId, 'name'],
                function (error, results) {
                  if (error) return;
                  var domainName = results.value;
                  count++;
                  domainList[count-1] = domainName;
                  //console.log("count = " + count + ": " + domainName);
                  if (count === maxDomains) ListDomainsResponse(domainList, SDB);
                }
              );
            }
          }
        }              
      }
    }
  );
};

var ListDomainsResponse = function(domainList, SDB) {
  var nvps = SDB.nvps;
  var action = nvps.Action;
  var isEmpty = (domainList.length === 0);
  var xml = responseStart({action: action, version: nvps.Version, hasContent: true});
  if (!isEmpty) {
    for (var i = 0; i < domainList.length; i++) {
      xml = xml + '<DomainName>' + domainList[i] + '</DomainName>';
    }
  }
  xml = xml + responseEnd(action, SDB.startTime, true);
  writeResponse(200, xml, SDB.response)
};

/*
 *****************  CreateDomain  ***************************
*/

var CreateDomain = function(SDB) {
  var error;
  var c;
  var nvps = SDB.nvps;
  var domainName = nvps.DomainName;
  if (domainName) {
    var allowed = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyx0123456789-_.';
    var nameOk = true;
    for (var i=0; i< domainName.length; i++) {
      c = domainName.charAt(i);
      if (allowed.indexOf(c) == -1) {
        nameOk = false;
        break;
      }
    }
    if (nameOk) {
      MDB.getGlobal([nvps.AWSAccessKeyId], function (error, results) {
        var noOfDomains = results.value;
        if ((noOfDomains === '')||(noOfDomains === '0')) {
          checkDomain(SDB);
        }
        else {
          db.clientPool[db.connection()].getGlobal('MDBConfig', ['DomainsPerAccount'], function (error, results) {
            var maxDomains = results.value;
            if (trace) console.log("maxDomains = " + maxDomains);
            if (maxDomains === '') {
              checkDomain(SDB);
            }
            else {
              if (noOfDomains === maxDomains) {
                //max no of domains already in database
                errorResponse('NumberDomainsExceeded', SDB)
              }
              else {
                checkDomain(SDB);
              }
            }
          });
        }
      });
    }
    else {
      error = {code:'InvalidParameterValue', message: 'Value (' + domainName + ') for parameter DomainName is invalid. ',status:400};
      returnError(SDB, error);
    }
  }
  else {
    error = {code:'MissingParameter', message: 'The request must contain the parameter DomainName',status:400};
    returnError(SDB, error);
  }
};

var checkDomain = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         okResponse(SDB);
       },
       notFound: function(SDB) {
         addDomain(SDB);
       }
  };
  checkDomainExists(invoke, SDB)
};

var addDomain = function(SDB) {
  if (trace) console.log("adding domain");
  var nvps = SDB.nvps;
  var accessKeyId = nvps.AWSAccessKeyId;
  var domainName = nvps.DomainName;
  var count = 0;
  MDB.increment([accessKeyId], 1, function (error, results) {
    count++;
    sendCreateDomainResponse(count, SDB);
  });
  MDB.increment([accessKeyId, 'domains'], 1, function (error, results) {
    var id = results.value;
    var now = new Date().getTime();
    var nameIndex = domainName.substring(0,MDB.indexLength);
    MDB.setGlobal([accessKeyId, 'domains', id, 'name'], domainName, function (error, results) {
      count++;
      sendCreateDomainResponse(count, SDB); 
    });
    MDB.setGlobal([accessKeyId, 'domains', id, 'created'], now, function (error, results) {
      count++;
      sendCreateDomainResponse(count, SDB);
    });
    MDB.setGlobal([accessKeyId, 'domains', id, 'modified'], now, function (error, results) {
      count++;
      sendCreateDomainResponse(count, SDB);
    });
    MDB.setGlobal([accessKeyId, 'domainIndex', nameIndex, id], '', function (error, results) {
      count++;
      sendCreateDomainResponse(count, SDB);
    });
  });
};

var sendCreateDomainResponse = function(count, SDB) {
  //if (trace) console.log("send response: count=" + count);
  //ensure that response isn't sent until ^MDB global fully updated
  if (count === 5) okResponse(SDB);
};


/*
 *****************  DeleteDomain  ***************************
*/

var DeleteDomain = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         removeDomain(index, SDB);
       },
       notFound: function(SDB) {
         okResponse(SDB)
       }
  };
  checkDomainExists(invoke, SDB)
};

var removeDomain = function(index, SDB) {
  var nvps = SDB.nvps;
  var domainName = nvps.DomainName;
  var accessKeyId = nvps.AWSAccessKeyId;
  if (trace) console.log("removing domain: index: " + index + "; name: " + domainName);
  var count = 0;
  var nameIndex = domainName.substring(0,MDB.indexLength);
  MDB.kill([accessKeyId, 'domains', index], function (error, results) {
    count++;
    sendDeleteDomainResponse(count, SDB);
  });
  MDB.kill([accessKeyId, 'domainIndex', nameIndex, index], function (error, results) {
    count++;
    sendDeleteDomainResponse(count, SDB);
  });
  MDB.decrement([accessKeyId], 1, function (error, results) {
    count++;
    sendDeleteDomainResponse(count, SDB);
  });
};

var sendDeleteDomainResponse = function(count, SDB) {
  //if (trace) console.log("send delete response: count=" + count);
  //ensure that response isn't sent until ^MDB global fully updated
  if (count === 3) okResponse(SDB);
};

/*
 *****************  DomainMetadata  ***************************
*/

var DomainMetadata = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         getMetadata(index, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB)
};

var getMetadata = function(index, SDB) {
  var metadata = {};
  var count = 0;
  var nvps = SDB.nvps;
  var accessKeyId = nvps.AWSAccessKeyId;

  // Timestamp

  MDB.getGlobal([accessKeyId, 'domains', index, 'modified'], function (error, results) {
    var timestamp = results.value;
    if (timestamp.indexOf(',') !== -1) {
      // convert timestamp from Mumps format
      var pieces = timestamp.split(',');
      timestamp = (parseInt(pieces[0]) * 86400) + parseInt(pieces[1]);
      timestamp = timestamp - 4070908800;
    }
    metadata.Timestamp = timestamp;
    count++;
    if (trace) console.log("timestamp - count=" + count);
    sendDomainMetadataResponse(metadata, count, SDB);
  });


  // ItemCount, AttributeValueCount & AttributeValuesSizeBytes
  MDB.getAllSubscripts([accessKeyId, 'domains', index, 'items'], function (error, results) {
    var noOfItems = results.length;
    if (trace) console.log("noOfItems = " + noOfItems);
    if (noOfItems === 0) {
      metadata.ItemNamesSizeBytes = 0;
      metadata.ItemCount = 0;
      metadata.AttributeValueCount = 0;
      metadata.AttributeValuesSizeBytes = 0;
      metadata.AttributeNameCount = 0;
      metadata.AttributeNamesSizeBytes = 0;
      count = count + 2;
      if (trace) console.log("ItemNamesSizeBytes - count=" + count);
      sendDomainMetadataResponse(metadata, count, SDB);
      return;
    };
    var itemId;
    metadata.ItemCount = noOfItems;
    metadata.ItemNamesSizeBytes = 0;
    metadata.AttributeValueCount = 0;
    metadata.AttributeValuesSizeBytes = 0;
    var itemsChecked = 0;
    var attributesChecked = 0;
    for (var i = 0; i < noOfItems; i++) {
      itemId = results[i];
      MDB.getGlobal([accessKeyId, 'domains', index, 'items', itemId], function (error, results) {
        var itemName = results.value + '';
        metadata.ItemNamesSizeBytes = metadata.ItemNamesSizeBytes + itemName.length;
        itemsChecked++;
        if (trace) console.log("fetched name of item " + itemsChecked + ": " + itemName + ": " + itemName.length + "; " + metadata.ItemNamesSizeBytes);
        if (itemsChecked === noOfItems) {
          count++;
          sendDomainMetadataResponse(metadata, count, SDB);
        }
      });
      MDB.getJSON([accessKeyId, 'domains', index, 'items', itemId], function (error, results) {
        if (trace) console.log("attributes : " + JSON.stringify(results));
        attributesChecked++;
        var attribs = results.attribs;
        if (trace) console.log("attribs: " + JSON.stringify(attribs));
        var index;
        var value;
        var valueNo;
        var attrib;
        var attribName;
        var attribValues;
        for (index in attribs) {
          attrib = attribs[index];
          attribValues = attrib['value'];
          for (valueNo in attribValues) {
            value = attribValues[valueNo] + '';
            //console.log("value = " + value);
            //console.log("value = " + value + "; length = " + value.length);
            metadata.AttributeValueCount++;
            metadata.AttributeValuesSizeBytes = metadata.AttributeValuesSizeBytes + value.length;
          }
        }
        if (attributesChecked === noOfItems) {
          count++;
          if (trace) console.log("AttributeValueCount - count=" + count);
          sendDomainMetadataResponse(metadata, count, SDB);
        }
      });
    }
  });


  // NameValueCount & NamesSizeBytes

  MDB.getGlobal([accessKeyId, 'domains', index, 'attribs'], function (error, results) {
    var noOfAttribs = parseInt(results.value);
    var attribName;
    metadata.AttributeNameCount = 0;
    metadata.AttributeNamesSizeBytes = 0;
    if (results.dataStatus === 0) {
      count++;
      sendDomainMetadataResponse(metadata, count, SDB);
    }
    else {
      MDB.getJSON([accessKeyId, 'domains', index, 'attribs'], function (error, results) {
        var attribNo;
        for (attribNo=1; attribNo <= noOfAttribs; attribNo++ ) {
          attribName = results[attribNo] + '';
          metadata.AttributeNameCount++;
          metadata.AttributeNamesSizeBytes = metadata.AttributeNamesSizeBytes + attribName.length;
          if (attribNo === noOfAttribs) {
            count++;
            sendDomainMetadataResponse(metadata, count, SDB);
          }
        }
      });
    }
  });
};


var sendDomainMetadataResponse = function(metadata, count, SDB) {
  if (trace) console.log("send metadata response: count=" + count);
  if (trace) console.log("metaData: " + JSON.stringify(metadata));
  //ensure that response isn't sent until all metadata items are collected
  if (count === 4) DomainMetadataResponse(metadata, SDB); // change to full response!
};

var DomainMetadataResponse = function(metadata, SDB) {
  var nvps = SDB.nvps;
  var action = nvps.Action;
  var xml = responseStart({action: action, version: nvps.Version, hasContent: true});
  var name;
  for (name in metadata) {
    xml = xml + '<' + name + '>' + metadata[name] + '</' + name + '>';
  }
  xml = xml + responseEnd(action, SDB.startTime, true);
  writeResponse(200, xml, SDB.response)
};


/*
 *****************  GetAttributes  ***************************
*/

var GetAttributes = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         getAttributes(index, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB)
};

var getAttributes = function(domainIndex, SDB) {
  var error;
  var nvps = SDB.nvps;
  var itemName = nvps.ItemName;
  var accessKeyId = nvps.AWSAccessKeyId;
  if (itemName) {
    // check if item name exists.  If it doesn't, return an empty set
    var index;
    var nameIndex = itemName.substring(0,MDB.indexLength);
    MDB.getJSON([accessKeyId, 'domains', domainIndex, 'itemIndex', nameIndex], function (error, results) {
      if (trace) console.log("itemIndex records: " + JSON.stringify(results));
      var matchFound = false;
      var count = 0;
      for (index in results) {
        count++;
      }
      if (count === 0) {
        // No matching name exists - return Empty set
        if (trace) console.log("ItemName not found - returning empty set");
        getAttributesResponse('', SDB);
      }
      else {
        if (trace) console.log("itemName may exist: checking..");
        var noChecked = 0;
        for (index in results) {
          MDB.getGlobal([accessKeyId, 'domains', domainIndex, 'items', index],
            function (error, results) {
              if (trace) console.log("** getGlobal: " + JSON.stringify(results));
              if (error) return;
              noChecked++;
              if (results.value === itemName) matchFound = true;
              if (noChecked === count) {
                if (trace) console.log("ItemName matchFound: " + matchFound + "; index = " + index);
                if (!matchFound) {
                  // No matching name exists
                  getAttributesResponse('', SDB);
                }
                else {
                  // item exists
                  // create attributes array containing all attributes for selected item
                  // first get list of required attribute names (otherwise all returned)
                  var attrsWanted = {};
                  var getAllAttrs = true;
                  var name;
                  for (name in nvps) {
                    if (name.indexOf("AttributeName") !== -1) {
                      attrsWanted[nvps[name]] = '';
                      getAllAttrs = false;
                    }
                  }
                  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'attribs'], function (error, results) {
                    var attribIndex = results;
                    MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', index, 'attribs'], function (error, results) {
                      var attribNo;
                      var attribName;
                      var values;
                      var valueNo;
                      var value;
                      var valueIndex;
                      var valueNo;
                      var valueNos;
                      var getAttr;
                      var attributes = [];
                      for (attribNo in results) {
                        if (parseInt(attribNo) !== 0) {
                          attribName = attribIndex[attribNo];
                          if (getAllAttrs) {
                            getAttr = true;
                          }
                          else {
                            getAttr = false;
                            if (attrsWanted[attribName] === '') getAttr = true;
                          }
                          if (getAttr) {
                            values = results[attribNo].valueIndex;
                            for (valueIndex in values) {
                              valueNos = values[valueIndex];
                              for (valueNo in valueNos) {
                                value = results[attribNo].value[valueNo];
                                attributes.push({name:attribName, value: value});
                              }
                            }
                          }
                        } 
                      }
                      getAttributesResponse(attributes, SDB);
                    });
                  });
                }
              }
            }
          );
        }
      }
    });
  }
  else {
    error = {code:'MissingParameter', message: 'The request must contain the parameter ItemName',status:400};
    returnError(SDB, error);
  }
};

var getAttributesResponse = function(attributes, SDB) {
  var nvps = SDB.nvps;
  var action = nvps.Action;
  if (attributes === '') {
    var xml = responseStart({action: action, version: nvps.Version, hasContent: false, isEmpty: true});
  }
  else {
    var xml = responseStart({action: action, version: nvps.Version, hasContent: true, isEmpty: false});
    var name;
    for (var i=0; i < attributes.length; i++) {
      xml = xml + "<Attribute>";
      xml = xml + '<Name>' + attributes[i].name + '</Name>';
      xml = xml + '<Value>' + xmlEscape(attributes[i].value) + '</Value>';
      xml = xml + "</Attribute>";
    }
  }
  var hasContent = (attributes !== '');
  xml = xml + responseEnd(action, SDB.startTime, hasContent);
  writeResponse(200, xml, SDB.response)
};

/*
 *****************  PutAttributes  ***************************
*/

var PutAttributes = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         putAttributes(index, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB);
};

var putAttributes = function(domainIndex, SDB) {
  var error = '';
  var nvps = SDB.nvps;
  var itemName = nvps.ItemName;
  var accessKeyId = nvps.AWSAccessKeyId;
  if (itemName) {
    // check Attribute & Expected name/value pairs.  Return error if invalid
    // creates SDB.attributes and SDB.expected objects
    error = checkAttrNVPs(SDB);
    if (error !== '') {
      returnError(SDB, error);
      return;
    }
    // check if item name exists.  If it doesn't, create it
    var invoke = {
       found: function(domainIndex, itemIndex, SDB) {
         addAttributes(domainIndex, itemIndex, SDB);
       },
       notFound: function(domainIndex, itemName, SDB) {
         addItemToDomain(domainIndex, itemName, SDB);
       }
    };
    checkItemExists(domainIndex, itemName, invoke, SDB);
  }
  else {
    error = {code:'MissingParameter', message: 'The request must contain the parameter ItemName',status:400};
    returnError(SDB, error);
  }
};

var addItemToDomain = function(domainIndex, itemName, SDB) {
  // if expected value is set, return attribute does not exist error
  if (SDB.expected.name) {
    error = {code:'AttributeDoesNotExist', message: 'Attribute (' + SDB.expected.name + ') does not exist',status:400};
    returnError(SDB, error);
  }
  else {
    var nvps = SDB.nvps;
    var accessKeyId = nvps.AWSAccessKeyId;
    MDB.increment([accessKeyId, 'domains', domainIndex, 'items'], 1, function (error, results) {
      var itemIndex = results.value;
      var itemNameIndex = itemName.substring(0,MDB.indexLength);
      var count = 0;
      var done = 7;
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'itemIndex', itemNameIndex, itemIndex], '', function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'items', itemIndex], itemName, function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'attribs', '0'], 'itemName()', function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'attribsIndex', 'itemName()', 0], '', function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', 0, 'value'], '1', function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', 0, 'value', 1], itemName, function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
      MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'queryIndex', '0', itemNameIndex, itemIndex], '', function (error, results) { 
        count++;
        if (count === done) addAttributes(domainIndex, itemIndex, SDB);
      });
    });
  }
};

var addAttributes = function(domainIndex, itemIndex, SDB) {
    var attributes = SDB.attributes;
    // check Expected value or exists
    if (typeof SDB.expected.name !== 'undefined') {
        if (trace) console.log("expected value for " + SDB.expected.name + ": " + SDB.expected.value);
        // Try to fetch value - if it's not the same as the expected value, send an error
        // if the attribute can't be found, send error
        // otherwise invoke addAttribs
        var attrName = SDB.expected.name;
        var invoke = {
          found: function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
            if (typeof SDB.expected.exists !== 'undefined') {
              if (!SDB.expected.exists) {
                // update if attribute doesn't exist, so return error
                error = {code:'ConditionalCheckFailed', message: 'Conditional check failed. Attribute (' + SDB.expected.name + ') value exists.',status:400};
                returnError(SDB, error);
              }
              else {
                checkExpectedValue(domainIndex, itemIndex, attrIndex, SDB)
              }
            }
            else {
              checkExpectedValue(domainIndex, itemIndex, attrIndex, SDB)
            }
          },
          notFound: function(domainIndex, itemIndex, attrName, SDB) {
            error = {code:'AttributeDoesNotExist', message: 'Attribute (' + SDB.expected.name + ') does not exist',status:400};
            returnError(SDB, error);
          }
        };

        checkAttributeExists(domainIndex, itemIndex, attrName, true, invoke, SDB);
    }
    else {
      addAttribs(domainIndex, itemIndex, attributes, SDB);
    }
};

var checkExpectedValue = function(domainIndex, itemIndex, attrIndex, SDB) {
  var attributes = SDB.attributes;
  MDB.getGlobal([SDB.nvps.AWSAccessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'value', 1], function (error, results) {
    if (trace) console.log("results.value = " + results.value + "; expected=" + SDB.expected.value);
    if (results.value !== SDB.expected.value) {
      error = {code:'ConditionalCheckFailed', message: 'Conditional check failed. Attribute (' + SDB.expected.name + ') value is (' + results.value + ') but was expected (' + SDB.expected.value + ')',status:400};
      returnError(SDB, error);
    }
    else {
      addAttribs(domainIndex, itemIndex, attributes, SDB);
    }
  });
};

var addAttribs = function(domainIndex, itemIndex, attributes, SDB) {
    if (trace) console.log("addAttributes: domainIndex = " + domainIndex + "; itemIndex = " + itemIndex);
    if (trace) console.log("attributes: " + JSON.stringify(attributes));

    var attrNo;
    var attribName;
    var attribValue;
    var expectedValue;
    var replace;
    var names = attributes.names;
    var values = attributes.values;
    var replaces = attributes.replaces;
    var attributeByName = {};
    var noOfValues = 0;
    for (attrNo in names) {
      noOfValues++;
      attribName = names[attrNo];
      attribValue = values[attrNo];
      replace = false;
      if (typeof replaces[attrNo] !== 'undefined') replace = replaces[attrNo];
      if (typeof attributeByName[attribName] === 'undefined') {
        attributeByName[attribName] = {values:[attribValue], replace:replace};
      }
      else {
        attributeByName[attribName].values.push(attribValue);
      }
    }

    if (trace) console.log("attributeByName = " + JSON.stringify(attributeByName));
    if (trace) console.log("no of values = " + noOfValues);
    SDB.max = noOfValues * 3;
    SDB.count = 0;
    if (trace) console.log("set SDB.max = " + SDB.max);
    SDB.attrProperties = {};
    SDB.checkAttr = {};
    for (attrName in attributeByName) {
      SDB.attrProperties[attrName] = attributeByName[attrName];
      addAttributeRecord(domainIndex, itemIndex, attrName, SDB);
    }
};

var addAttributeRecord = function(domainIndex, itemIndex, attribName, SDB) {
  if (trace) console.log("addAttributeRecord; attribName = " + attribName);
  var invoke = {
    found: function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
      if (trace) console.log(attribName + " found - call addAttribute. attrIndex = " + attrIndex);
      addAttribute(domainIndex, itemIndex, attrIndex, attrName, SDB);
    },
    notFound: function(domainIndex, itemIndex, attrName, SDB) {
      // first create new attribute index, then add attribute
      if (trace) console.log(attribName + " not found - call addAttributeIndexToDomain");
      addAttributeIndexToDomain(domainIndex, itemIndex, attrName, SDB);
    }
  };
  // check to see if this attribute exists at all in the domain
  // false means we don't check the attribute exists for the item
  checkAttributeExists(domainIndex, itemIndex, attribName, false, invoke, SDB);
};

var addAttributeIndexToDomain = function(domainIndex, itemIndex, attrName, SDB) {
  if (trace) console.log("add attribute index for attr " + attrName + " to domain");
  var nvps = SDB.nvps;
  var accessKeyId = nvps.AWSAccessKeyId;
  MDB.increment([accessKeyId, 'domains', domainIndex, 'attribs'], 1, function (error, results) {
    var attrIndex = results.value;
    var attrNameIndex = attrName.substring(0,MDB.indexLength);
    var count = 0;
    MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'attribs', attrIndex], attrName, function (error, results) { 
      count++;
      if (count === 2) addAttribute(domainIndex, itemIndex, attrIndex, attrName, SDB);
    });
    MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'attribsIndex', attrNameIndex, attrIndex], '', function (error, results) { 
      count++;
      if (count === 2) addAttribute(domainIndex, itemIndex, attrIndex, attrName, SDB);
    });
  });
};

var addAttribute = function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
    var attrProperties = SDB.attrProperties[attrName];
    if (trace) console.log("add attribute values for attrindex " + attrIndex);
    if (trace) console.log("attrProperties = " + JSON.stringify(attrProperties));
    var accessKeyId = SDB.nvps.AWSAccessKeyId;
    var values = attrProperties.values;
    var value;
    var replace = attrProperties.replace;
    if (replace) {
      // remove any existing value(s) for this attribute
      removeOldValues(domainIndex, itemIndex, attrIndex, values, SDB);
    }
    else {
      addNewAttrValues(domainIndex, itemIndex, attrIndex, values, SDB);
    }
};

var removeOldValues = function(domainIndex, itemIndex, attrIndex, values, SDB) {
    var accessKeyId = SDB.nvps.AWSAccessKeyId;
      MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', itemIndex, "attribs", attrIndex, 'value'], function (error, results) {
        if (trace) console.log("existing attr records: " + JSON.stringify(results));
        var count = 0;
        var max = 0;
        var value;
        var valueNameIndex;
        for (valueIndex in results) {
          max++;
        }
        max = (max * 2) +1;
        if (trace) console.log("removing: max = " + max);
        for (valueIndex in results) {
          value = results[valueIndex] + '';
          valueNameIndex = value.substring(0,MDB.indexLength);
          if (trace) console.log("removing valueIndex " + valueIndex + "; attrIndex=" + attrIndex + "; valueNameIndex = " + valueNameIndex + "; itemIndex = " + itemIndex);
          MDB.kill([accessKeyId, 'domains', domainIndex, 'items', itemIndex, "attribs", attrIndex, 'valueIndex', value, valueIndex], function (error, results) {
            count++;
            if (trace) console.log("1 count = " + count);
            if (count === max) addNewAttrValues(domainIndex, itemIndex, attrIndex, values, SDB);
          });
          MDB.kill([accessKeyId, 'domains', domainIndex, 'queryIndex', attrIndex, valueNameIndex, itemIndex], function (error, results) {
            count++;
            if (trace) console.log("2 count = " + count);
            if (count === max) addNewAttrValues(domainIndex, itemIndex, attrIndex, values, SDB);
          });
        }
        MDB.kill([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'value'], function (error, results) {
          count++;
          if (trace) console.log("3 count = " + count);
          if (count === max) addNewAttrValues(domainIndex, itemIndex, attrIndex, values, SDB);
        });
      });
};

var addNewAttrValues = function(domainIndex, itemIndex, attrIndex, values, SDB) {
  if (trace) console.log("addNewAttrValues: attrIndex = " + attrIndex);
  var nvps = SDB.nvps;
  var value;
  if (trace) console.log("values.length = " + values.length);
  var max = values.length * 3;
  var valueNameIndex;
  var accessKeyId = nvps.AWSAccessKeyId;
  for (var i = 0; i < values.length; i++) {
    value = values[i];
    addNewAttrValue(max, domainIndex, itemIndex, attrIndex, value, SDB);
  }
};

var addNewAttrValue = function(max, domainIndex, itemIndex, attrIndex, value, SDB) {
  if (trace) console.log("addNewAttrValue: attrIndex = " + attrIndex + "; value= " + value);
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  var valueNameIndex = value.substring(0,MDB.indexLength);
  //console.log("addNewAttrValue: valueNameIndex = " + valueNameIndex);

  // if this value already exists for this attribute, don't do anything (increment count by 3 though!)

  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', itemIndex, "attribs", attrIndex], function (error, results) {
    if (trace) console.log("matching records for the attribute and value: " + JSON.stringify(results));
    var values;
    var exists = false;
    if (typeof results.valueIndex !== 'undefined') {
      if (typeof results.valueIndex[valueNameIndex] !== 'undefined') {
        values = results.valueIndex[valueNameIndex];
        for (valueNo in values) {
          if (typeof results.value !== 'undefined') {
            if (typeof results.value[valueNo] !== 'undefined') {
               if ((value + '') === (results.value[valueNo] + '')) {
                 if (trace) console.log("value '" + value + "' already exists for this attribute!");
                 exists = true;
                 break;
               }
            }
          }
        }
      }
    }
    if (exists) {
      SDB.count = SDB.count + 3;
      if (SDB.count === SDB.max) sendPutAttributesResponse(domainIndex, SDB);
    }
    else {
      MDB.increment([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'value'], 1, function (error, results) {
        var valueIndex = results.value;
        MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'value', valueIndex], value, function (error, results) { 
          SDB.count++;
          if (trace) console.log("1: " + SDB.count + "; " + SDB.max + "; valueIndex=" + valueIndex);
          if (SDB.count === SDB.max) sendPutAttributesResponse(domainIndex, SDB);
        });
        MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'valueIndex', valueNameIndex, valueIndex], '', function (error, results) { 
          SDB.count++;
          if (trace) console.log("2: " + SDB.count + "; " + SDB.max + "; valueIndex=" + valueIndex);
          if (SDB.count === SDB.max) sendPutAttributesResponse(domainIndex, SDB);
        });
        if (trace) console.log("setting queryIndex: " + domainIndex + ";" + attrIndex + "; " + valueNameIndex + "; " + itemIndex);
        MDB.setGlobal([accessKeyId, 'domains', domainIndex, 'queryIndex', attrIndex, valueNameIndex, itemIndex], '', function (error, results) { 
          SDB.count++;
          if (trace) console.log("3: " + SDB.count + "; " + SDB.max + "; valueIndex=" + valueIndex);
          if (SDB.count === SDB.max) sendPutAttributesResponse(domainIndex, SDB);
        });
      });
    }
  });
};

var sendPutAttributesResponse = function(domainIndex, SDB, counter) {
  if (SDB.nvps.Action === 'PutAttributes') {
    okResponse(SDB);
  }
  else {
    // BatchPutAttributes - process next item unless exhausted the list
    
    //counter.count++;
    //if (trace) console.log("BatchAttributes: finished adding itemNo " + counter.count);
    //if (counter.count === counter.max) {
    //  okResponse(SDB);
    //}

    SDB.batchItemNo++;
    if (trace) console.log("BatchAttributes: finished adding itemNo " + SDB.batchItemNo);
    if (SDB.batchItemNo === SDB.batchItemNos.length) {
      okResponse(SDB);
    }
    else {
      putItem(domainIndex, SDB.batchItemNo, SDB);
    }

  }
};


/*
 *****************  BatchPutAttributes  ***************************
*/

var BatchPutAttributes = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         parseBatchPutAttributes(index, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB);
};

var parseBatchPutAttributes = function(domainIndex, SDB) {
  var error = '';
  var nvps = SDB.nvps;
  var noOfItems = 0;
  var pieces;
  var name;
  var itemNo;
  var paramName;
  var param = {};
  var attr;
  var attrs;
  var attrNo;
  var attrParam;
  var noOfAttrs;
  var itemName;
  for (name in nvps) {
    if (name.indexOf("Item.") !== -1) {
      noOfItems++;
      pieces = name.split('.');
      itemNo = pieces[1];
      if (typeof param[itemNo] === 'undefined') param[itemNo] = {};
      paramName = pieces[2];
      if (paramName === 'ItemName') {
        param[itemNo].itemName = nvps[name];
      }
      if (paramName === 'Attribute') {
        attrNo = pieces[3];
        if (typeof param[itemNo].attrs === 'undefined') param[itemNo].attrs = {};
        var attrParam = pieces[4];
        if (attrParam === 'Name') {
          if (typeof param[itemNo].attrs[attrNo] === 'undefined') param[itemNo].attrs[attrNo] = {};
          param[itemNo].attrs[attrNo].name = nvps[name];
        }       
        if (attrParam === 'Value') {
          if (typeof param[itemNo].attrs[attrNo] === 'undefined') param[itemNo].attrs[attrNo] = {};
          param[itemNo].attrs[attrNo].value = nvps[name];
        } 
        if (attrParam === 'Replace') {
          if (typeof param[itemNo].attrs[attrNo] === 'undefined') param[itemNo].attrs[attrNo] = {};
          param[itemNo].attrs[attrNo].replace = nvps[name];
        } 
      }
    }
  }
  if (noOfItems === 0) {
    error = {code:'MissingParameter', message: 'The request must contain the parameter ItemName',status:400};
    returnError(SDB, error);    
  }
  else {
    // now check the params for consistency
    var error = '';
    var itemNames = {};
    for (itemNo in param) {
      if (typeof param[itemNo].itemName === 'undefined') {
        error = {code:'MissingParameter', message:'The request must contain the parameter ItemName'};
        break;
      }
      itemName = param[itemNo].itemName;
      if (typeof itemNames[itemName] !== 'undefined') {
        error = {code:'DuplicateItemName', message:'Item ' + itemName + ' was specified more than once'};
        break;
      }
      itemNames[itemName] = '';
      attrs = param[itemNo].attrs;
      if (typeof attrs === 'undefined') {
        error = {code:'MissingParameter', message:'No attributes for item ' + itemName};
        break;
      }
      noOfAttrs = 0;
      for (attrNo in attrs) {
        attr = attrs[attrNo];
        if ((typeof attr.name === 'undefined')&&(typeof attr.value !== 'undefined')) {
          error = {code:'MissingParameter', message:'Attribute.Name missing for Attribute.Value=' + attr.value};
          break;
        }
        if ((typeof attr.name !== 'undefined')&&(typeof attr.value === 'undefined')) {
          error = {code:'MissingParameter', message:'Attribute.Value missing for Attribute.Name=' + attr.name};
          break;
        }
        if (typeof attr.replace !== 'undefined') {
          if ((attr.replace !== 'true')&&(attr.replace !== 'false')) {
            error = {code:'InvalidParameterValue', message:'Value ' + attr.replace + ' for parameter Replace is invalid. The Replace flag should be either true or false.'};
            break;
          }
        }
        noOfAttrs++;
      }
      if (error !== '') break;
      if (noOfAttrs === 0) {
        error = {code:'MissingParameter', message:'No attributes for item ' + itemName};
        break;
      }
    }
    if (error !== '') {
      error.status = 400;
      returnError(SDB, error);  
    }
    else {
      // BatchPutAttributes request is OK - start processing it
      batchPutAttributes(domainIndex, param, SDB);
    }
  }
};

var batchPutAttributes = function(domainIndex, param, SDB) {
  var itemNos = [];
  var count = 0;
  for (itemNo in param) {
    itemNos[count] = itemNo;
    count++;
  }
  SDB.batchItemCount = 0;
  SDB.batchItemNos = itemNos;
  SDB.batchParams = param;
  putItem(domainIndex, 0, SDB);
};

var putItem = function(domainIndex, itemNo, SDB) {
  var itemName;
  var itemNo;
  var invoke;
  var attrs;
  var attr;
  var attrNo;
  var param = SDB.batchParams;
  itemName = param[itemNo].itemName;
  if (trace) console.log("x itemName = " + itemName);
  // map the item properties to the format used by PutAttributes

  attrs = param[itemNo].attrs;
  attributes = {
    X:{},
    names:{},
    values:{},
    replaces:{}
  };
  SDB.expected = {};
  for (attrNo in attrs) {
    attr = attrs[attrNo];
    attributes.X[attrNo] = '';
    attributes.names[attrNo] = attr.name;
    attributes.values[attrNo] = attr.value;
    if (attr.replace === 'true') attributes.replaces[attrNo] = true;
  }
  SDB.attributes = attributes;
  SDB.batchItemNo = itemNo;
 
  // Now invoke the standard PutAttributes code for a single item
  // check if item name exists.  If it doesn't, create it
  invoke = {
    found: function(domainIndex, itemIndex, SDB) {
      addAttributes(domainIndex, itemIndex, SDB);
    },
    notFound: function(domainIndex, itemName, SDB) {
      addItemToDomain(domainIndex, itemName, SDB);
    }
  };
  checkItemExists(domainIndex, itemName, invoke, SDB);
};



/*
 *****************  DeleteAttributes  ***************************
*/

var DeleteAttributes = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         deleteAttributes(index, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB)
};


var deleteAttributes = function(domainIndex, SDB) {
  var error = '';
  var nvps = SDB.nvps;
  var itemName = nvps.ItemName;
  var accessKeyId = nvps.AWSAccessKeyId;
  if (itemName) {
    // Check Attribute name/value pairs - function will return errors if invalid and go no further
    // creates SDB.attributes and SDB.expected objects
    error = checkAttrNVPs(SDB);
    if (error !== '') {
      returnError(SDB, error);
      return;
    }
    // check if item name exists.  If it doesn't, just send OK message
    var invoke = {
       found: function(domainIndex, itemIndex, SDB) {
         deleteAttrsConditions(domainIndex, itemIndex, SDB);
       },
       notFound: function(domainIndex, itemName, SDB) {
         okResponse(SDB);
         return;
       }
    };
    checkItemExists(domainIndex, itemName, invoke, SDB);
  }
  else {
    error = {code:'MissingParameter', message: 'The request must contain the parameter ItemName',status:400};
    returnError(SDB, error);
    return;
  }
};

var deleteAttrsConditions = function(domainIndex, itemIndex, SDB) {
    // check Expected value or exists
    if (typeof SDB.expected.name !== 'undefined') {
        if (trace) console.log("expected value for " + SDB.expected.name + ": " + SDB.expected.value);
        // Try to fetch value - if it's not the same as the expected value, send an error
        // if the attribute can't be found, send error
        // otherwise invoke addAttribs
        var attrName = SDB.expected.name;
        var invoke = {
          found: function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
            if (typeof SDB.expected.exists !== 'undefined') {
              if (!SDB.expected.exists) {
                if (trace) console.log("condition failed - attribute doesn't exist");
                // update if attribute doesn't exist, so return error
                error = {code:'ConditionalCheckFailed', message: 'Conditional check failed. Attribute (' + SDB.expected.name + ') value exists.',status:400};
                returnError(SDB, error);
              }
              else {
                if (trace) console.log("Condition passed - attribute exists");
                deleteAttrsCheckExpectedValue(domainIndex, itemIndex, attrIndex, SDB)
              }
            }
            else {
              if (trace) console.log("Moving on to check expected attribute value");
              deleteAttrsCheckExpectedValue(domainIndex, itemIndex, attrIndex, SDB)
            }
          },
          notFound: function(domainIndex, itemIndex, attrName, SDB) {
            if (trace) console.log("error - conditional attribute does not exist");
            error = {code:'AttributeDoesNotExist', message: 'Attribute (' + SDB.expected.name + ') does not exist',status:400};
            returnError(SDB, error);
          }
        };

        checkAttributeExists(domainIndex, itemIndex, attrName, true, invoke, SDB);
    }
    else {
      if (trace) console.log("No Expected condition");
      removeItemOrAttributes(domainIndex, itemIndex, SDB);
    }
};

var deleteAttrsCheckExpectedValue = function(domainIndex, itemIndex, attrIndex, SDB) {
  MDB.getGlobal([SDB.nvps.AWSAccessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'value', 1], function (error, results) {
    if (trace) console.log("results.value = " + results.value + "; expected=" + SDB.expected.value);
    if (results.value !== SDB.expected.value) {
      if (trace) console.log("Condition failed - values don't match");
      error = {code:'ConditionalCheckFailed', message: 'Conditional check failed. Attribute (' + SDB.expected.name + ') value is (' + results.value + ') but was expected (' + SDB.expected.value + ')',status:400};
      returnError(SDB, error);
    }
    else {
      if (trace) console.log("Condition passed - values match");
      removeItemOrAttributes(domainIndex, itemIndex, SDB);
    }
  });
};


var removeItemOrAttributes = function(domainIndex, itemIndex, SDB) {
  // If only itemName is specified, remove entire item, subjected to expected conditions
  if (trace) console.log("in removeItemOrAttributes");
  if (trace) console.log("attributes: " + JSON.stringify(SDB.attributes));
  if (trace) console.log("expected: " + JSON.stringify(SDB.expected));
  var noOfAttributes = 0;
  var attributes = SDB.attributes;
  for (attrName in attributes.names) {
    noOfAttributes++;
    break;
  }
  if (noOfAttributes === 0) {
    removeItem(domainIndex, itemIndex, SDB);
  }
  else {
    if (trace) console.log("deleting specified attributes.  If none left, remove item too");
    removeAttributes(domainIndex, itemIndex, SDB);
  }
};

var removeItem = function(domainIndex, itemIndex, SDB) {
  var nvps = SDB.nvps;
  var itemName = nvps.ItemName;
  var attribId;
  var attribObj;
  var value;
  var valueIndex;
  var itemNameIndex = itemName.substring(0,MDB.indexLength);
  var accessKeyId = nvps.AWSAccessKeyId;
  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', itemIndex, "attribs"], function (error, results) {
    if (trace) console.log("item records: " + JSON.stringify(results));
    var max = 0;
    // count how many values to remove from queryIndex records
    for (attribId in results) {
      attribObj = results[attribId];
      if (typeof attribObj.valueIndex !== 'undefined') {
        for (value in attribObj.valueIndex) {
          max++;
        }
      }
    }
    var count = 0;
    var killCount = 0;
    if (trace) console.log(max + " records to be deleted");
    for (attribId in results) {
      attribObj = results[attribId];
      if (typeof attribObj.valueIndex !== 'undefined') {
        for (value in attribObj.valueIndex) {
          MDB.kill([accessKeyId, 'domains', domainIndex, 'queryIndex', attribId, value, itemIndex], function (error, results) {
            count++;
            if (trace) console.log("deleted record: count=" + count);
            if (count === max) {
              MDB.kill([accessKeyId, 'domains', domainIndex, 'items', itemIndex], function (error, results) {
                killCount++;
                if (killCount === 3) removeItemResponse(SDB);
              });
              MDB.kill([accessKeyId, 'domains', domainIndex, 'itemIndex', itemNameIndex, itemIndex], function (error, results) {
                killCount++;
                if (killCount === 3) removeItemResponse(SDB);
              });
              MDB.kill([accessKeyId, 'domains', domainIndex, 'queryIndex', '0', itemNameIndex], function (error, results) {
                killCount++;
                if (killCount === 3) removeItemResponse(SDB);
              });
            }
          });
        }
      }
    }
  });
};

var removeItemResponse = function(SDB) {
  if (SDB.nvps.Action === 'DeleteAttributes') {
    okResponse(SDB);
  }
  else {
    if (trace) console.log("BatchDeleteAttributes: finished deleting itemNo " + SDB.batchItemNo);
    SDB.batchItemNo++;
    if (SDB.batchItemNo === SDB.batchItemNos.length) {
      okResponse(SDB);
    }
    else {
      deleteItem(SDB.batchDomainIndex, SDB.batchItemNo, SDB);
    }
  }
};

var removeAttributes = function(domainIndex, itemIndex, SDB) {
  if (trace) console.log("removeAttributes");
  var nvps = SDB.nvps;
  var itemName = nvps.ItemName;

  var attributes = SDB.attributes;
  var attrNo;
  var attribName;
  var attribValue;
  var attrValue;
  var replace;
  var names = attributes.names;
  var values = attributes.values;
  var value;
  var attributeByName = {};
  if (trace) console.log("name = " + JSON.stringify(names) + " ****");
  for (attrNo in names) {
    attribName = names[attrNo];
    attrValue = '';
    if (typeof values[attrNo] !== 'undefined') {
      attribValue = values[attrNo];
      attrValue = {values:[attribValue]};
    }
    if (typeof attributeByName[attribName] === 'undefined') {
      attributeByName[attribName] = attrValue;
    }
    else {
      if (attrValue !== '') {
        attributeByName[attribName].values.push(attribValue);
      }
    }
  }
  if (trace) console.log("attributeByName: " + JSON.stringify(attributeByName));
  var noOfValues = 0;
  for (attribName in attributeByName) {
    value = attributeByName[attribName];
    if (value !== '') {
      // count each value to be processed
      var val;
      for (val in value) noOfValues++;
    }
    else {
      noOfValues++;
    }
  }
  SDB.NoOfAttributesToDelete = noOfValues;
  SDB.NoOfAttributesDeleted = 0;
  for (attribName in attributeByName) {
    value = attributeByName[attribName];
    if (value !== '') {
      // delete specified values for attribute
      if (typeof SDB.attributeValues === 'undefined') SDB.attributeValues = {};
      SDB.attributeValues[attribName] = value.values;
    }
    deleteAttribute(domainIndex, itemIndex, attribName, SDB);
  }
};

var deleteAttribute = function(domainIndex, itemIndex, attrName, SDB) {
  if (trace) console.log("in deleteAttribute: itemIndex = " + itemIndex + "; attrName = " + attrName);
  // find attribute index
  var invoke = {
    found: function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
      //delete Attribute
      if (trace) console.log("found attribute index: " + attrIndex);
      removeAttribute(domainIndex, itemIndex, attrIndex, attrName, SDB);
    },
    notFound: function(domainIndex, itemIndex, attrName, SDB) {
      // no action - just flag as if attribute deleted
      // DeleteAttributes is idempotent
      sendDeleteAttributesResponse(SDB);
    }
  };
  checkAttributeExists(domainIndex, itemIndex, attrName, true, invoke, SDB);
};

var removeAttribute = function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
  // get all values, first delete each one's queryIndex reference
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  var value;
  var valueNameIndex;
  var valueIndex;
  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', itemIndex, "attribs", attrIndex], function (error, results) {
    if (trace) console.log(attrName + ": attr records: " + JSON.stringify(results));
    if (typeof results.valueIndex !== 'undefined') {
      if (trace) console.log(attrName + ": valueIndex defined");
      var noOfValues = 0;
      valueIndex = results.valueIndex;
      for (valueNameIndex in valueIndex) noOfValues++;
      var counter = {
        count: 0,
        noOfValues: noOfValues
      };
      var deleteAttr;
      var attrIndices;
      var attrix;
      var specifiedValues;
      var specifiedValueArray;
      for (valueNameIndex in valueIndex) {
        specifiedValues = '';
        attrIndices = valueIndex[valueNameIndex];
        value = '';
        for (attrix in attrIndices) {
          value = results.value[attrix];
          break;
        }
        if (typeof SDB.attributeValues !== 'undefined') {
          if (typeof SDB.attributeValues[attrName] !== 'undefined') {
            specifiedValues = {};
            specifiedValueArray = SDB.attributeValues[attrName];
            for (var i=0; i < specifiedValueArray.length; i++) {
              specifiedValues[specifiedValueArray[i]] = '';
            }
            if (trace) console.log("specifiedValues = " + JSON.stringify(specifiedValues));
            if (value in specifiedValues) {
              if (trace) console.log("specified value '" + value + "' matches value in database, so delete it!");
              if (trace) console.log("itemIndex: " + itemIndex + "; attribIndex: " + attrIndex + "; valueIndex: " + attrix);
              removeValueFromAttr(domainIndex, itemIndex, attrIndex, attrName, attrix, valueNameIndex, SDB, counter);
            }
            else {
              if (trace) console.log("value in database - '" + value + " isn't in the specified list of values");
              counter.count++;
              if (counter.count === counter.noOfValues) sendDeleteAttributesResponse(SDB);
            }
          }
          else {
            if (trace) console.log(attrName + " ** deleting - value = " + value);
            removeValueFromAttr(domainIndex, itemIndex, attrIndex, attrName, attrix, valueNameIndex, SDB, counter);
          }
        }
        else {
          if (trace) console.log("!! deleting - value = " + value);
          removeValueFromAttr(domainIndex, itemIndex, attrIndex, attrName, attrix, valueNameIndex, SDB, counter);
        }
      }
    }
  });
};

var removeValueFromAttr = function(domainIndex, itemIndex, attrIndex, attrName, valueIndex, valueNameIndex, SDB, counter) {
  if (trace) console.log("removeValueFromAttr: " + domainIndex + "; " + itemIndex + "; " + attrIndex + "; " + valueIndex + "; " + valueNameIndex);
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  var valRecCount = 0;
  MDB.kill([accessKeyId, 'domains', domainIndex, 'queryIndex', attrIndex, valueNameIndex, itemIndex], function (error, results) {
    valRecCount++;
    if (valRecCount == 3) {
      counter.count++;
      if (counter.count === counter.noOfValues) removeAttrFromItem(domainIndex, itemIndex, attrIndex, attrName, SDB);
    }
  });
  MDB.kill([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'value', valueIndex], function (error, results) {
    valRecCount++;
    if (valRecCount == 3) {
      counter.count++;
      if (counter.count === counter.noOfValues) removeAttrFromItem(domainIndex, itemIndex, attrIndex, attrName, SDB);
    }
  });
  MDB.kill([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex, 'valueIndex', valueNameIndex, valueIndex], function (error, results) {
    valRecCount++;
    if (valRecCount == 3) {
      counter.count++;
      if (counter.count === counter.noOfValues) removeAttrFromItem(domainIndex, itemIndex, attrIndex, attrName, SDB);
    }
  });
};


var removeAttrFromItem = function(domainIndex, itemIndex, attrIndex, attrName, SDB) {
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  if (trace) console.log("possibly removing " + attrName + " from " + SDB.nvps.ItemName + "; attrIndex = " + attrIndex);
  //db.clientPool[db.connection()].kill('MDB', [accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs', attrIndex], function (error, results) {
    // if no items refer to this attribute, remove the attribute from domain
    MDB.getJSON([accessKeyId, 'domains', domainIndex, 'queryIndex', attrIndex], function (error, results) {
      if (trace) console.log("queryIndex records: " + JSON.stringify(results));
      var attrStillReferenced = false;
      var value;
      for (value in results) {
        attrStillReferenced = true;
        break;
      }
      if (!attrStillReferenced) {
        // remove attribute from domain
        // then, if item has no attributes left, remove the item from domain
        if (trace) console.log("Removing " + attrName + " from " + SDB.nvps.ItemName + "; attrIndex = " + attrIndex);
        var count = 0;
        var attrNameIndex = attrName.substring(0,MDB.indexLength);
        MDB.kill([accessKeyId, 'domains', domainIndex, 'attribs', attrIndex], function (error, results) {
          count++;
          if (count == 2) removeItemFromDomain(domainIndex, itemIndex, SDB);
        });
        MDB.kill([accessKeyId, 'domains', domainIndex, 'attribsIndex', attrNameIndex], function (error, results) {
          count++;
          if (count == 2) removeItemFromDomain(domainIndex, itemIndex, SDB);
        });
      }
      else {
        // see if item has any attributes left. if not, delete the item
        removeItemFromDomain(domainIndex, itemIndex, SDB);
      }
    });
  //});
};

var removeItemFromDomain = function(domainIndex, itemIndex, SDB) {
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  if (trace) console.log("possibly removing " + SDB.nvps.ItemName + " from " + SDB.nvps.DomainName);
  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'items', itemIndex, 'attribs'], function (error, results) {
    if (trace) console.log("attribs records: " + JSON.stringify(results));
    var itemStillReferenced = false;
    delete results['0'];
    var value;
    var itemNo;
    for (itemNo in results) {
      if (trace) console.log(itemNo + ": " + JSON.stringify(results[itemNo]));
      if (typeof results[itemNo].valueIndex !== 'undefined') {
        itemStillReferenced = true;
        break;
      }
    }
    if (!itemStillReferenced) {
      // remove item from domain
      var count = 0;
      if (trace) console.log("Removing " + SDB.nvps.ItemName + " from " + SDB.nvps.DomainName);
      var itemNameIndex = SDB.nvps.ItemName.substring(0,MDB.indexLength);
      MDB.kill([accessKeyId, 'domains', domainIndex, 'items', itemIndex], function (error, results) {
        count++;
        if (count == 3) sendDeleteAttributesResponse(SDB);
      });
      MDB.kill([accessKeyId, 'domains', domainIndex, 'itemIndex', itemNameIndex, itemIndex], function (error, results) {
        count++;
        if (count == 3) sendDeleteAttributesResponse(SDB);
      });
      MDB.kill([accessKeyId, 'domains', domainIndex, 'queryIndex', '0', itemNameIndex], function (error, results) {
        count++;
        if (count == 3) sendDeleteAttributesResponse(SDB);
      });
    }
    else {
      sendDeleteAttributesResponse(SDB);
    }
  });
};

var sendDeleteAttributesResponse = function(SDB) {
  SDB.NoOfAttributesDeleted++;
  if (trace) console.log("No of Attributes deleted = " + SDB.NoOfAttributesDeleted + " out of " + SDB.NoOfAttributesToDelete);
  if (SDB.NoOfAttributesDeleted === SDB.NoOfAttributesToDelete) {
    if (SDB.nvps.Action === 'DeleteAttributes') {
      okResponse(SDB);
    }
    else {
      if (trace) console.log("BatchDeleteAttributes: finished deleting itemNo " + SDB.batchItemNo);
      SDB.batchItemNo++;
      if (SDB.batchItemNo === SDB.batchItemNos.length) {
        okResponse(SDB);
      }
      else {
        deleteItem(SDB.batchDomainIndex, SDB.batchItemNo, SDB);
      }
    }
  }

};

/*
 *****************  BatchDeleteAttributes  ***************************
*/

var BatchDeleteAttributes = function(SDB) {
  var invoke = {
       found: function(index, SDB) {
         parseBatchDeleteAttributes(index, SDB);
       },
       notFound: function(SDB) {
         errorResponse('NoSuchDomain', SDB);
       }
  };
  checkDomainExists(invoke, SDB);
};

var parseBatchDeleteAttributes = function(domainIndex, SDB) {
  var error = '';
  var nvps = SDB.nvps;
  var noOfItems = 0;
  var pieces;
  var name;
  var itemNo;
  var paramName;
  var param = {};
  var attr;
  var attrs;
  var attrNo;
  var attrParam;
  var noOfAttrs;
  var itemName;
  for (name in nvps) {
    if (name.indexOf("Item.") !== -1) {
      noOfItems++;
      pieces = name.split('.');
      itemNo = pieces[1];
      if (typeof param[itemNo] === 'undefined') param[itemNo] = {};
      paramName = pieces[2];
      if (paramName === 'ItemName') {
        param[itemNo].itemName = nvps[name];
      }
      if (paramName === 'Attribute') {
        attrNo = pieces[3];
        if (typeof param[itemNo].attrs === 'undefined') param[itemNo].attrs = {};
        var attrParam = pieces[4];
        if (attrParam === 'Name') {
          if (typeof param[itemNo].attrs[attrNo] === 'undefined') param[itemNo].attrs[attrNo] = {};
          param[itemNo].attrs[attrNo].name = nvps[name];
        }       
        if (attrParam === 'Value') {
          if (typeof param[itemNo].attrs[attrNo] === 'undefined') param[itemNo].attrs[attrNo] = {};
          param[itemNo].attrs[attrNo].value = nvps[name];
        } 
      }
    }
  }
  if (noOfItems === 0) {
    error = {code:'MissingParameter', message: 'The request must contain the parameter ItemName',status:400};
    returnError(SDB, error);    
  }
  else {
    // now check the params for consistency
    var error = '';
    var itemNames = {};
    for (itemNo in param) {
      if (typeof param[itemNo].itemName === 'undefined') {
        error = {code:'MissingParameter', message:'The request must contain the parameter ItemName'};
        break;
      }
      itemName = param[itemNo].itemName;
      if (typeof itemNames[itemName] !== 'undefined') {
        error = {code:'DuplicateItemName', message:'Item ' + itemName + ' was specified more than once'};
        break;
      }
      itemNames[itemName] = '';
      attrs = param[itemNo].attrs;
      for (attrNo in attrs) {
        attr = attrs[attrNo];
        if ((typeof attr.value !== 'undefined')&&(attr.value === '')) {
          error = {code:'InvalidParameterValue', message:'Value (' + attr.value + ') for parameter Name is invalid. The empty string is an illegal attribute name'};
          break;
        }
      }
      if (error !== '') break;
    }
    if (error !== '') {
      error.status = 400;
      returnError(SDB, error);  
    }
    else {
      // BatchDeleteAttributes request is OK - start processing it
      batchDeleteAttributes(domainIndex, param, SDB);
    }
  }
};

var batchDeleteAttributes = function(domainIndex, param, SDB) {
  var itemNos = [];
  var count = 0;
  for (itemNo in param) {
    itemNos[count] = itemNo;
    count++;
  }
  SDB.batchItemCount = 0;
  SDB.batchItemNos = itemNos;
  SDB.batchParams = param;
  SDB.batchDomainIndex = domainIndex;
  deleteItem(domainIndex, 0, SDB);
};

var deleteItem = function(domainIndex, itemNo, SDB) {
  var itemName;
  var itemNo;
  var invoke;
  var attrs;
  var attr;
  var attrNo;
  var param = SDB.batchParams;
  itemName = param[itemNo].itemName;
  if (trace) console.log("itemName = " + itemName);
  // map the item properties to the format used by PutAttributes

  attrs = param[itemNo].attrs;
  attributes = {
    X:{},
    names:{},
    values:{},
    replaces:{}
  };
  SDB.expected = {};
  for (attrNo in attrs) {
    attr = attrs[attrNo];
    attributes.X[attrNo] = '';
    attributes.names[attrNo] = attr.name;
    attributes.values[attrNo] = attr.value;
    if (attr.replace === 'true') attributes.replaces[attrNo] = true;
  }
  if (typeof SDB.attributeValues !== 'undefined') delete SDB.attributeValues;
  SDB.attributes = attributes;
  SDB.batchItemNo = itemNo;
  SDB.nvps.ItemName = itemName;
 
  // Now invoke the standard DeleteAttributes code for a single item
  var invoke = {
    found: function(domainIndex, itemIndex, SDB) {
      removeItemOrAttributes(domainIndex, itemIndex, SDB);
    },
    notFound: function(domainIndex, itemName, SDB) {
      okResponse(SDB);
      return;
    }
  };
  checkItemExists(domainIndex, itemName, invoke, SDB);
};


/*
 *****************  End of Actions  **************************
*/

/*
 *****************  Action Support Functions  **************
*/


 // invoke = {'found': function(..){..}, 'notFound': function(..){..}}

var checkDomainExists = function(invoke, SDB) {
  // Check if Domain exists and invoke found or notFound functions
  var error;
  var nvps = SDB.nvps;
  var domainName = nvps.DomainName;
  var accessKeyId = nvps.AWSAccessKeyId;
  if (domainName) {
    // check if domain name already exists.  If it doesn't, call the domainExists function
    var index;
    var index1;
    var nameIndex = domainName.substring(0,MDB.indexLength);
    MDB.getJSON([accessKeyId, 'domainIndex', nameIndex], function (error, results) {
      if (trace) console.log("domainIndex records: " + JSON.stringify(results));
      var matchFound = false;
      var count = 0;
      for (index in results) {
        count++;
        index1 = index;
      }
      if (count === 0) {
        // No matching name exists - just send the OK response
        if (trace) console.log("domain name not found");
        invoke.notFound(SDB);
      }
      else if (count === 1) {
        if (trace) console.log("one matching domain index found: " + index1);
        invoke.found(index1, SDB);
      }
      else {
        //console.log("multiple possible matching domain names..");
        var noChecked = 0;
        for (index in results) {
          MDB.getGlobal([accessKeyId, 'domains', index, 'name'],
            function (error, results) {
              //console.log("getGlobal: " + JSON.stringify(results));
              if (error) return;
              noChecked++;
              if (results.value === domainName) matchFound = true;
              if (noChecked === count) {
                //console.log("matchFound: " + matchFound + "; index = " + index);
                if (!matchFound) {
                  // No matching name exists
                  invoke.notFound(SDB);
                }
                else {
                  invoke.found(index, SDB);
                }
              }
            }
          );
        }
      }
    });
  }
  else {
    error = {code:'MissingParameter', message: 'The request must contain the parameter DomainName',status:400};
    returnError(SDB, error);
  }
};

var checkItemExists = function(domainIndex, itemName, invoke, SDB) {
  // Check if named Item exists and invoke found or notFound functions
  var index;
  var index1;
  var nameIndex = itemName.substring(0,MDB.indexLength);
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'itemIndex', nameIndex], function (error, results) {
    if (trace) console.log("itemIndex records: " + JSON.stringify(results));
    var matchFound = false;
    var count = 0;
    for (index in results) {
      count++;
      index1 = index;
    }
    if (count === 0) {
      // No matching name exists - add new item to domain
      invoke.notFound(domainIndex, itemName, SDB);
    }
    else if (count === 1) {
      if (trace) console.log("one matching item index found: " + index1);
      invoke.found(domainIndex, index1, SDB);
    }
    else {
      if (trace) console.log("itemName may exist: checking..");
      var noChecked = 0;
      for (index in results) {
        MDB.getGlobal([accessKeyId, 'domains', domainIndex, 'items', index],
          function (error, results) {
            if (error) return;
            noChecked++;
            if (results.value === itemName) matchFound = true;
            if (noChecked === count) {
              if (trace) console.log("ItemName matchFound: " + matchFound + "; index = " + index);
              if (!matchFound) {
                // No matching name exists
                invoke.notFound(domainIndex, itemName, SDB);
              }
              else {
                // item exists - add attributes
                invoke.found(domainIndex, index, SDB);
              }
            }
          }
        );
      }
    }
  });
};


var checkAttributeExists = function(domainIndex, itemIndex, attributeName, inItem, invoke, SDB) {
  //inItem = true = check the attribute exists for the specified item
  //         false = check if the attribute exists in the domain
  var nameIndex = attributeName.substring(0, MDB.indexLength);
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  var index;
  var index1;
  MDB.getJSON([accessKeyId, 'domains', domainIndex, 'attribsIndex', nameIndex], function (error, results) {
    if (trace) console.log("attribsIndex records: " + JSON.stringify(results));
    var matchFound = false;
    var count = 0;
    for (index in results) {
      count++;
      index1 = index;
    }
    if (count === 0) {
      // No matching name exists - invoke notFound function
      invoke.notFound(domainIndex, itemIndex, attributeName, SDB);
    }
    else if (count === 1) {
      if (trace) console.log("one matching attribute index found: " + index1);
      invoke.found(domainIndex, itemIndex, index1, attributeName, SDB);
    }
    else {
      if (trace) console.log("attributeName " + attributeName + " may exist: checking..");
      if (typeof SDB.checkAttr === 'undefined') SDB.checkAttr = {};
      SDB.checkAttr[attributeName] = {
        noOfAttrsChecked: 0,
        noAttrsToCheck: count,
        matchingAttrFound: false,
        attrIndex: ''
      };
      for (index in results) {
        checkAttrIndex(inItem, domainIndex, itemIndex, attributeName, index, invoke, SDB)
      }
    }
  });
};

var checkAttrIndex = function(inItem, domainIndex, itemIndex, attributeName, attrIndex, invoke, SDB) {
  var accessKeyId = SDB.nvps.AWSAccessKeyId;
  MDB.getGlobal([accessKeyId, 'domains', domainIndex, 'attribs', attrIndex],function (error, results) {
    SDB.checkAttr[attributeName].noOfAttrsChecked++;
    if (results.value === attributeName) {
      SDB.checkAttr[attributeName].matchingAttrFound = true;
      SDB.checkAttr[attributeName].attrIndex = attrIndex;
    }
    if (SDB.checkAttr[attributeName].noOfAttrsChecked === SDB.checkAttr[attributeName].noAttrsToCheck) {
      if (trace) console.log("matchFound for " + attributeName + ": " + SDB.checkAttr[attributeName].matchingAttrFound + "; attrIndex = " + attrIndex);
      if (SDB.checkAttr[attributeName].matchingAttrFound) {
        if (inItem) {
          // attribute index found. Now does the specified item have this attribute?
          MDB.getGlobal([accessKeyId, 'domains', domainIndex, 'items', itemIndex, "attribs", SDB.checkAttr[attributeName].attrIndex],function (error, results) {
            if (trace) console.log("dataStatus = " + results.dataStatus);
            if (results.dataStatus === 0) {
              //  Specified item doesn't have this attribute
              invoke.notFound(domainIndex, itemIndex, attributeName, SDB); 
            }
            else {
              invoke.found(domainIndex, itemIndex, SDB.checkAttr[attributeName].attrIndex, attributeName, SDB);
            }
          });
        }
        else {
          invoke.found(domainIndex, itemIndex, SDB.checkAttr[attributeName].attrIndex, attributeName, SDB);
        }
      }
      else {
        // No matching name exists - invoke notFound function
        invoke.notFound(domainIndex, itemIndex, attributeName, SDB);
      }              }
    }
  );
};

var checkAttrNVPs = function(SDB) {
  if (trace) console.log("in checkAttrNVPs");
  // Check Attribute name/value pairs - make sure at least one present and they're all formatted OK
  var error = '';
  var nvps = SDB.nvps;
  var pieces;
  var attrNo;
  var attributes = {
    X: {},
    names: {},
    values: {},
    replaces: {}
  };
  var name;
  var replace;
  var exists;
  var noOfExpecteds = 0;
  var expected = {};
  for (name in nvps) {
    if (name.indexOf("Attribute") !== -1) {
      if (name.indexOf('.') === -1) {
        error = {code:'MissingParameter', message: 'No attributes',status:400};
        break;
      }
      pieces = name.split('.');
      if (pieces.length !== 3) {
        error = {code:'MissingParameter', message: 'No attributes',status:400};
        break;
      }
      attrNo = pieces[1];
      attributes.X[attrNo] = '';
      if (pieces[2] === 'Name') {
        attributes.names[attrNo] = nvps[name];
      }
      if (pieces[2] === 'Value') {
        attributes.values[attrNo] = nvps[name];
      }
      if (nvps.Action === "PutAttributes") {
        if (pieces[2] === 'Replace') {
          replace = nvps[name];
          if ((replace !== 'true')&&(replace !== 'false')) {
            error = {code:'InvalidParameterValue', message: 'Value (' + replace + ') for parameter Replace is invalid. The Replace flag should be either true or false.',status:400};
            break;
          }
          else {
            attributes.replaces[attrNo] = (replace === 'true');
          }
        }
      }
    }
    if (name.indexOf("Expected") !== -1) {
      if (name.indexOf('.') === -1) {
        error = {code:'InvalidParameterValue', message: 'Parameter (' + name + ') is not recognized',status:400};
        break;
      }
      pieces = name.split('.');
      if (pieces.length !== 3) {
        error = {code:'InvalidParameterValue', message: 'Parameter (' + name + ') is not recognized',status:400};
        break;
      }
      attrNo = pieces[1];
      if (pieces[2] === 'Name') {
        noOfExpecteds++;
        if (noOfExpecteds > 1) {
          error = {code:'MultipleExpectedValues', message: 'Only one ExpectedValue can be specified',status:400};
          break;
        }
        else {
          expected.name = nvps[name];
        }
      }
      if (pieces[2] === 'Value') {
        expected.value = nvps[name];
      }
      if (pieces[2] === 'Exists') {
        exists = nvps[name];
        if ((exists !== 'true')&&(exists !== 'false')) {
          error = {code:'InvalidParameterValue', message: 'Value (' + exists + ') for parameter Expected.Exists is invalid. Expected.Exists should be either true or false.',status:400};
          break;
        }
        else {
          expected.exists = (exists === 'true');
        }
      }
    }
  }
  if (error !== '') return error;

  if ((typeof expected.value !== 'undefined') && (typeof expected.exists !== 'undefined')) {
    if (!expected.exists) {
      error = {code:'ExistsAndExpectedValue', message: "Expected.Exists=false and Expected.Value cannot be specified together",status:400};
      return error;
    }
  }
  if ((typeof expected.value === 'undefined') && (typeof expected.exists !== 'undefined')) {
    if (expected.exists) {
      error = {code:'IncompleteExpectedExpression', message: "If Expected.Exists=true or unspecified, then Expected.Value has to be specified",status:400};
      return error;
    }
  }
  if (nvps.Action === 'PutAttributes') {
    for (attrNo in attributes.X) {
      if ((typeof attributes.names[attrNo] !== 'undefined')&&(typeof attributes.values[attrNo] === 'undefined')) {
        error = {code:'MissingParameter', message: "Attribute.Value missing for Attribute.Name='" + attributes.names[attrNo] + "'",status:400};
        break;
      }
      if ((typeof attributes.names[attrNo] === 'undefined')&&(typeof attributes.values[attrNo] !== 'undefined')) {
        error = {code:'MissingParameter', message: "Attribute.Name missing for Attribute.Value='" + attributes.values[attrNo] + "'",status:400};
        break;
      }
    }
  }
  if (error !== '') return error;

  if (trace) console.log("attributes: " + JSON.stringify(attributes));
  SDB.attributes = attributes;
  SDB.expected = expected;
  return error;
};

/*
 *****************  Request Processing Support Functions  **************
*/

var parseContent = function(contentString) {
  // Transfer request name/value pairs into an object for easier processing
  var nvpObject = {};
  var pair;
  var nvps = contentString.split('&');
  for ( param in nvps ){
    pair = nvps[param].split('=') ;
    nvpObject[decode(pair[0])] = decode(pair[1]);
  }
  return nvpObject;
};

var decode = function(str) {
 // ensures that + is translated to space correctly during URI decode
 // essential for AWS Java SDB client!
 //console.log("decoding " + str + ":");
 //console.log("decodeURI: " + decodeURI(str.replace(/\+/g, " ")));
 //console.log("decodeURIComponent: " + decodeURIComponent(str.replace(/\+/g, " ")));
 //console.log("unescape: " + unescape(str.replace(/\+/g, " ")));
 //return unescape(str.replace(/\+/g, " "));
 try {
    return decodeURIComponent(str.replace(/\+/g, " "));
 }
 catch (err) {
   return unescape(str.replace(/\+/g, " "));
 }
};

/*
*************** SDB HTTP Response generator functions ****************
*/

var returnError = function(SDB, error) {
  // Generic SDB Error Response function
  var xml = '<Response><Errors><Error>';
  xml = xml + '<Code>' + error.code + '</Code>';
  xml = xml + '<Message>' + error.message + '</Message>';
  xml = xml + boxUsage(SDB.startTime);
  xml = xml + '</Error></Errors>';
  xml = xml + requestId();
  xml = xml + '</Response>\n';
  writeResponse(error.status, xml, SDB.response)
};

var writeResponse = function(status, xml, response) {
  responseHeader(status, response);
  response.write(xml);
  response.end();
};

var responseHeader = function(status, response) {
  var dateNow = new Date();
  response.writeHead(status, {"Server": "Amazon SimpleDB", "Content-Type": "text/xml", "Date": dateNow.toUTCString()}); 
  response.write('<?xml version="1.0"?>\n');
};

var requestId = function() {
  return '<RequestID>' + uuid().toLowerCase() + '</RequestID>';
};

var boxUsage = function(startTime) {
  var now = new Date().getTime();
  var elapsed = (now - startTime)
  if (elapsed === 0) elapsed = 0.005;
  elapsed = elapsed/1000;
  elapsed = elapsed.toFixed(10);
  if (!silentStart) console.log("BoxUsage: " + elapsed);
  return '<BoxUsage>' + elapsed + '</BoxUsage>';
};

var metaData = function(startTime) {
  return '<ResponseMetadata>' + requestId() + boxUsage(startTime) + '</ResponseMetadata>';
};

var responseStart = function(params) {
  var response = '<' + params.action + 'Response xmlns="http://sdb.amazonaws.com/doc/' + params.version + '/">';
  if (params.hasContent) response = response + '<' + params.action + 'Result>';
  if (params.isEmpty) response = response + '<' + params.action + 'Result/>';
  return response;
};

var responseEnd = function(action, startTime, hasContent) {
  var response = '';
  if (hasContent) response = '</' + action + 'Result>';
  response = response + metaData(startTime) + '</' + action + 'Response>\n';
  return response;
};

var okResponse = function(SDB) {
  // Generic response to be used where no data is returned
  var nvps = SDB.nvps;
  var xml = responseStart({action: nvps.Action, version: nvps.Version});
  xml = xml + responseEnd(nvps.Action, SDB.startTime, false);
  writeResponse(200, xml, SDB.response)
};

var xmlEscape = function(xml) {
  xml = xml + '';
  return xml.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
};

/*
************* Error Responses *******************
*/

var errorResponse = function(type, SDB) {
  var error = {code: type, status:400};
  switch(type) {
    case 'NoSuchDomain':
      error.message = 'The specified domain does not exist.';
      break;
    case 'NumberDomainsExceeded':
      error.message = 'Number of domains limit exceeded';
      break;
    case 'SignatureDoesNotMatch':
      error.message = 'The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.';
      break;
    case 'MissingAction':
      error.message = 'No action was supplied with this request';
      break;
  }
  returnError(SDB, error);
};

/*
************* Signature Crypto Functions *******************
*/

var createStringToSign = function(SDB, includePort, encodeType) {
  var stringToSign;
  var request = SDB.request;
  var nvps = SDB.nvps;
  switch(nvps.SignatureVersion) {
    case '0':
      // to do
      break;
    case '1':
      // to do
      break;
    case '2':
      var name;
      var amp = '';
      var value;
      var keys = [];
      var index = 0;
      var pieces;
      var host = request.headers.host;
      if (!includePort) { 
        if (host.indexOf(":") !== -1) {
          pieces = host.split(":");
          host = pieces[0];
        }
      }
      var url = request.url;
      if (request.method === 'GET') {
        pieces = request.url.split("?");
        url = pieces[0];
      } 
      stringToSign = request.method + '\n' + host + '\n' + url + '\n';
      for (name in nvps) {
        if (name !== 'Signature') {
          keys[index] = name;
          index++;
        }
      }
      keys.sort();
      for (var i=0; i < keys.length; i++) {
        name = keys[i];
        value = nvps[name];
        //console.log("name = " + name + "; value = " + value);
        stringToSign = stringToSign + amp + sdbEscape(name, encodeType) + '=' + sdbEscape(value, encodeType);
        amp = '&';
      }
      break;
    default:
      // Invalid signature version
      stringToSign = -1;
  }
  return stringToSign;
};

var sdbEscape = function(string, encode) {
  if (encode === "escape") {
    var unreserved = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~';
    var escString = '';
    var c;
    var hex;
    for (var i=0; i< string.length; i++) {
      c = string.charAt(i);
      if (unreserved.indexOf(c) !== -1) {
        escString = escString + c;
      }
      else {
        hex = string.charCodeAt(i).toString(16).toUpperCase();
        //console.log(string + "; c=" + c + "; hex = " + hex);
        if (hex.length === 1) hex = '0' + hex;
        escString = escString + '%' + hex;
      }
    }
    return escString;
  }
  else {
    var enc = encodeURIComponent(string);
    return enc.replace(/\*/g, "%2A").replace(/\'/g, "%27").replace(/\!/g, "%21").replace(/\(/g, "%28").replace(/\)/g, "%29");
  }
};

var digest = function(string, key, type) {
  // type = sha1|sha25|sha512
  var hmac = crypto.createHmac(type, key);
  hmac.update(string);
  return hmac.digest('base64');
};


/*
********** Confirmation that M/DB Service has Started ***********
*/

if (!silentStart) {
  console.log("M/DB has started successfully on port " + httpPort);
  if (trace) {
    console.log("Trace mode is on");
  }
  else {
    console.log("Trace mode is off");
  }
}

// Waiting message - needed for EC2 servers to prevent freezing

setInterval(function() { 
  console.log("Waiting..." + new Date().toLocaleTimeString());
},120000);

