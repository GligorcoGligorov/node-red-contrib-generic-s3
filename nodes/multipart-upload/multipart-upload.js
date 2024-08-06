const {CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand} = require("@aws-sdk/client-s3");


module.exports = function MultipartUpload(RED) {
  const nodeInstance = instanceNode(RED);
  RED.nodes.registerType("Multipart Upload", nodeInstance);
};

function instanceNode(RED) {
  return function nodeInstance(n) {
    RED.nodes.createNode(this, n); // Getting options for the current node
    this.conf = RED.nodes.getNode(n.conf); // Getting configuration
    let config = this.conf ? this.conf : null; // Cheking if the conf is valid
    if (!config) {
      this.warn(RED._("Missing S3 Client Configuration!"));
      return;
    }
    // Bucket parameter
    this.bucket = n.bucket !== "" ? n.bucket : null;
    // Object key parameter
    this.key = n.key !== "" ? n.key : null;
    // Body of the object to upload
    this.body = n.body !== "" ? n.body : null;

    // Input Handler
    this.on("input", inputHandler(this, RED));
  };
}


function inputHandler(n, RED) {
  return async function nodeInputHandler(msg, send, done) {
    const { S3 } = require("@aws-sdk/client-s3");
    const { isString, stringToStream } = require("../../common/common");
    const crypto = require("crypto");

    let msgClone;
    try {
      msgClone = structuredClone(msg);
    } catch (e) {
      msg.error = e;
      this.error(e, e);
      return;
    }

    // Retrieve properties from node configuration or message
    let bucket = n.bucket || msgClone.bucket;
    if (!bucket) {
      this.error("No bucket provided!");
      return;
    }

    let key = n.key || msgClone.key;
    if (!key) {
      this.error("No object key provided!");
      return;
    }

    let body = n.body || msgClone.body;
    if (!body) {
      this.error("No body data provided to put in the object!");
      return;
    }

    // Convert body to a string if it's an object
    if (typeof body === 'object') {
      body = JSON.stringify(body);
    }

    if (!isString(body) && !Buffer.isBuffer(body)) {
      this.error("The body should be a string or Buffer!");
      return;
    }

    // S3 client init
    let s3Client = null;
    try {
      s3Client = new S3({
        endpoint: n.conf.endpoint,
        forcePathStyle: n.conf.forcepathstyle,
        region: n.conf.region,
        credentials: {
          accessKeyId: n.conf.credentials.accesskeyid,
          secretAccessKey: n.conf.credentials.secretaccesskey,
        },
      });

      this.status({ fill: "blue", shape: "dot", text: "Uploading" });
      const bodyStream = Buffer.isBuffer(body) ? stringToStream(body) : stringToStream(body);

      const multipartUpload = await s3Client.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }));
      const uploadId = multipartUpload.UploadId;

      const chunkSize = 5 * 1024 * 1024; // 5MB chunks
      const parts = [];
      let partNumber = 1;
      let chunk;

      while (null !== (chunk = bodyStream.read(chunkSize))) {
        const uploadPartResponse = await s3Client.send(new UploadPartCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
          Body: chunk,
          PartNumber: partNumber
        }));

        parts.push({ ETag: uploadPartResponse.ETag, PartNumber: partNumber });
        partNumber++;
      }

      const completeUpload = await s3Client.send(new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts }
      }));

      msgClone.payload = completeUpload;
      msgClone.key = key;

      send(msgClone);
      this.status({ fill: "green", shape: "dot", text: "Success" });

    } catch (err) {
      this.status({ fill: "red", shape: "dot", text: "Failure" });
      msgClone.payload = null;
      msgClone.error = err;
      msgClone.key = key;
      this.error(err, msgClone);
      send(msgClone);
    } finally {
      if (s3Client) s3Client.destroy();
      setTimeout(() => {
        this.status({});
      }, 3000);
      done();
    }
  };
}
