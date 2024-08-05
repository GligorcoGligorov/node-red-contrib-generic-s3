module.exports = function MultipartUpload(RED) {
  function Node(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const s3Client = new require("@aws-sdk/client-s3").S3Client();

    node.on('input', async function(msg, send, done) {
      const { CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } = require("@aws-sdk/client-s3");

      const bucketName = config.bucket || msg.bucket;
      const key = config.key || msg.key;
      const parts = config.parts ? JSON.parse(config.parts) : msg.parts;

      if (!bucketName || !key || !parts || !Array.isArray(parts)) {
        node.error("Missing required parameters", msg);
        return done();
      }

      let uploadId;
      try {
        const multipartUpload = await s3Client.send(new CreateMultipartUploadCommand({ Bucket: bucketName, Key: key }));
        uploadId = multipartUpload.UploadId;

        const uploadPromises = parts.map((part, index) => {
          return s3Client.send(new UploadPartCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: uploadId,
            Body: Buffer.from(part.body, 'utf8'),
            PartNumber: index + 1
          })).then(data => ({ ETag: data.ETag, PartNumber: index + 1 }));
        });

        const uploadResults = await Promise.all(uploadPromises);

        const completeUpload = await s3Client.send(new CompleteMultipartUploadCommand({
          Bucket: bucketName,
          Key: key,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: uploadResults
          }
        }));

        msg.payload = completeUpload;
        send(msg);
        node.status({ fill: "green", shape: "dot", text: "Upload complete" });
      } catch (error) {
        if (uploadId) {
          await s3Client.send(new AbortMultipartUploadCommand({ Bucket: bucketName, Key: key, UploadId: uploadId }));
        }
        node.error(error, msg);
        node.status({ fill: "red", shape: "dot", text: "Upload failed" });
        done(error);
      }
    });
  }
  RED.nodes.registerType("Multipart Upload", Node);
};
