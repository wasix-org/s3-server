use s3_server::dto::{
    ByteStream, CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateBucketRequest, CreateMultipartUploadRequest, UploadPartRequest,
};
use s3_server::storages::fs::FileSystem;
use s3_server::S3Storage;

use std::path::PathBuf;

use uuid::Uuid;

fn setup_fs() -> (PathBuf, FileSystem) {
    let root = std::env::temp_dir().join(format!("s3-server-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let fs = FileSystem::new(&root).unwrap();
    (root, fs)
}

#[tokio::test]
async fn complete_multipart_upload_creates_parent_dirs() {
    let (root, fs) = setup_fs();

    let bucket = "test-bucket";
    // A prefixed key whose parent dirs do not exist yet; CompleteMultipartUpload
    // must create them instead of failing File::create with NotFound.
    let key = "simple/pkg/x.whl";

    fs.create_bucket(CreateBucketRequest {
        bucket: bucket.to_owned(),
        ..Default::default()
    })
    .await
    .unwrap();

    let upload = fs
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();
    let upload_id = upload.upload_id.unwrap();

    let content = b"hello multipart".to_vec();
    let part = fs
        .upload_part(UploadPartRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(ByteStream::from(content.clone())),
            ..Default::default()
        })
        .await
        .unwrap();

    let output = fs
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    e_tag: part.e_tag,
                    part_number: Some(1),
                }]),
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(output.e_tag.is_some());

    let written = std::fs::read(root.join(bucket).join(key)).unwrap();
    assert_eq!(written, content);

    std::fs::remove_dir_all(&root).ok();
}
