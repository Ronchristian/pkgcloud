/*
 * files.js: Instance methods for working with files from AWS S3
 *
 * (C) 2012 Charlie Robbins, Ken Perkins, Ross Kukulinski & the Contributors.
 *
 */

var base = require('../../../core/storage'),
  pkgcloud = require('../../../../../lib/pkgcloud'),
  through = require('through2'),
  storage = pkgcloud.providers.amazon.storage,
  _ = require('lodash');

//
// ### function removeFile (container, file, callback)
// #### @container {string} Name of the container to destroy the file in
// #### @file {string} Name of the file to destroy.
// #### @callback {function} Continuation to respond to when complete.
// Destroys the `file` in the specified `container`.
//
exports.removeFile = function (container, file, callback) {
  var self = this;

  if (container instanceof storage.Container) {
    container = container.name;
  }

  if (file instanceof storage.File) {
    file = file.name;
  }

  self.s3.deleteObject({
    Bucket: container,
    Key: file
  }, function (err, data) {
    return err ?
      callback(err) :
      callback(null, !!data.DeleteMarker);
  });
};

exports.upload = function (options) {
  var self = this;

  // check for deprecated calling with a callback
  if (typeof arguments[arguments.length - 1] === 'function') {
    self.emit('log::warn', 'storage.upload no longer supports calling with a callback');
  }

  var s3Options = {
    Bucket: options.container instanceof base.Container ? options.container.name : options.container,
    Key: options.remote instanceof base.File ? options.remote.name : options.remote
  };

  var s3Settings = {
    queueSize: options.queueSize || 1,
    partSize: options.partSize || 5 * 1024 * 1024
  };

  if (options.cacheControl) {
    s3Options.CacheControl = options.cacheControl;
  }

  if (options.contentType) {
    s3Options.ContentType = options.contentType;
  }

  // use ACL until a more obvious permission generalization is available
  if (options.acl) {
    s3Options.ACL = options.acl;
  }

  // add AWS specific options
  if (options.cacheControl) {
    s3Options.CacheControl = options.cacheControl;
  }

  if (options.ServerSideEncryption) {
    s3Options.ServerSideEncryption = options.ServerSideEncryption;
  }

  // we need a writable stream because aws-sdk listens for an error event on writable
  // stream and redirects it to the provided callback - without the writable stream
  // the error would be emitted twice on the returned proxyStream
  var writableStream = through();
  // we need a proxy stream so we can always return a file model
  // via the 'success' event
  var proxyStream = through();

  s3Options.Body = writableStream;

  var managedUpload = self.s3.upload(s3Options, s3Settings);

  proxyStream.managedUpload = managedUpload;

  managedUpload.send(function (err, data) {
    if (err) {
      return proxyStream.emit('error', err);
    }
    return proxyStream.emit('success', new storage.File(self, data));
  });

  proxyStream.pipe(writableStream);

  return proxyStream;
};

exports.download = function (options) {
  var self = this;

  return self.s3.getObject({
    Bucket: options.container instanceof base.Container ? options.container.name : options.container,
    Key: options.remote instanceof base.File ? options.remote.name : options.remote
  }).createReadStream();

};

exports.getFile = function (container, file, callback) {
  var containerName = container instanceof base.Container ? container.name : container,
    self = this;

  self.s3.headObject({
    Bucket: containerName,
    Key: file
  }, function (err, data) {
    return err ?
      callback(err) :
      callback(null, new storage.File(self, _.extend(data, {
        container: container,
        name: file
      })));
  });
};

exports.getFiles = function (container, options, callback) {
  var containerName = container instanceof base.Container ? container.name : container,
    self = this;

  if (typeof options === 'function') {
    callback = options;
    options = {};
  } else if (!options) {
    options = {};
  }

  var s3Options = {
    Bucket: containerName
  };

  if (options.marker) {
    s3Options.Marker = options.marker;
  }

  if (options.prefix) {
    s3Options.Prefix = options.prefix;
  }

  if (options.maxKeys) {
    s3Options.MaxKeys = options.maxKeys;
  }

  self.s3.listObjects(s3Options, function (err, data) {
    return err ?
      callback(err) :
      callback(null, self._toArray(data.Contents).map(function (file) {
        file.container = container;
        return new storage.File(self, file);
      }), {
        isTruncated: data.IsTruncated,
        marker: data.Marker,
        nextMarker: data.NextMarker
      });
  });
};

exports.createMultiPartUpload = function (options, callback) {
  var container = options.container instanceof base.Container ? options.container.name : options.container;
  var remote = options.remote instanceof base.File ? options.remote.name : options.remote;

  var multiPartParams = {
    Bucket: container,
    Key: remote,
    ContentType: options.contentType
  };

  var multipartMap = {
    Parts: []
  };

  var self = this,
    partNum = 0,
    uploadedParts = 0,
    streamEnded = false,
    partSize = options.partSize || 1024 * 1024 * 5, // Minimum 5MB per chunk (except the last part) http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
    chunkedStream = new storage.ChunkedStream(partSize),
    proxyStream = through(),

    uploadPart = function (multipart, partParams) {
      self.s3.uploadPart(partParams, function (multiErr, mData) {
        if (multiErr) {
          proxyStream.emit('error', multiErr);
          return;
        }

        multipartMap.Parts[this.request.params.PartNumber - 1] = {
          ETag: mData.ETag,
          PartNumber: Number(this.request.params.PartNumber)
        };

        uploadedParts++;

        // If the incoming readstream is completed and all parts are uploaded, end the outgoing writestream as well
        if (streamEnded && uploadedParts === partNum) {
          proxyStream.emit('end', {
            uploadId: multipart.UploadId,
            multipartMap: multipartMap
          });
        }
      });
    };

  // Create multipart upload
  self.s3.createMultipartUpload(multiPartParams, function (mpErr, multipart) {
    if (mpErr) {
      return callback(mpErr);
    }

    // return the stream using which the consumer can pipe the data
    chunkedStream.on('data', function (data) {
      partNum++;
      var partParams = {
        Body: data,
        Bucket: container,
        Key: remote,
        PartNumber: String(partNum),
        UploadId: multipart.UploadId
      };

      uploadPart(multipart, partParams);
    });

    chunkedStream.on('end', function () {
      streamEnded = true;
    });

    chunkedStream.on('error', function (err) {
      proxyStream.emit('error', err);
    });

    proxyStream.pipe(chunkedStream);

    return callback(null, proxyStream);
  });
};

exports.completeMultiPartUpload = function (options, callback) {
  var self = this;

  var doneParams = {
    Bucket: options.container instanceof base.Container ? options.container.name : options.container,
    Key: options.remote instanceof base.File ? options.remote.name : options.remote,
    MultipartUpload: options.multipartMap,
    UploadId: options.uploadId
  };

  self.s3.completeMultipartUpload(doneParams, function (err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, data);
    }
  });
};