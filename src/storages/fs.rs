//! fs implementation

use crate::async_trait;
use crate::data_structures::BytesStream;
use crate::dto::{
    Bucket, CompleteMultipartUploadError, CompleteMultipartUploadOutput,
    CompleteMultipartUploadRequest, CopyObjectError, CopyObjectOutput, CopyObjectRequest,
    CopyObjectResult, CreateBucketError, CreateBucketOutput, CreateBucketRequest,
    CreateMultipartUploadError, CreateMultipartUploadOutput, CreateMultipartUploadRequest,
    DeleteBucketError, DeleteBucketOutput, DeleteBucketRequest, DeleteObjectError,
    DeleteObjectOutput, DeleteObjectRequest, DeleteObjectsError, DeleteObjectsOutput,
    DeleteObjectsRequest, DeletedObject, GetBucketLocationError, GetBucketLocationOutput,
    GetBucketLocationRequest, GetObjectError, GetObjectOutput, GetObjectRequest, HeadBucketError,
    HeadBucketOutput, HeadBucketRequest, HeadObjectError, HeadObjectOutput, HeadObjectRequest,
    ListBucketsError, ListBucketsOutput, ListBucketsRequest, ListObjectsError, ListObjectsOutput,
    ListObjectsRequest, ListObjectsV2Error, ListObjectsV2Output, ListObjectsV2Request, Object,
    PutObjectError, PutObjectOutput, PutObjectRequest, UploadPartError, UploadPartOutput,
    UploadPartRequest,
};
use crate::errors::{S3StorageError, S3StorageResult};
use crate::headers::{AmzCopySource, Range};
use crate::path::S3Path;
use crate::storage::S3Storage;
use crate::utils::{crypto, time, Apply};

use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use futures::stream::{Stream, StreamExt, TryStreamExt};
use hyper::body::Bytes;
use md5::{Digest, Md5};
use path_absolutize::Absolutize;
use rusoto_s3::CommonPrefix;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tracing::{debug, error};
use uuid::Uuid;

use tokio::fs;
use tokio::fs::File;

/// A S3 storage implementation based on file system
#[derive(Debug)]
pub struct FileSystem {
    /// root path
    root: PathBuf,
}

/// Helper function to recursively collect all files and directories
async fn collect_files_recursive(dir: &Path, files: &mut Vec<(PathBuf, bool)>) -> io::Result<()> {
    let mut stack = vec![dir.to_owned()];

    while let Some(current_dir) = stack.pop() {
        let mut entries = fs::read_dir(&current_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;

            if file_type.is_dir() {
                // Add the directory itself
                files.push((path.clone(), true));
                // Add to stack for processing
                stack.push(path);
            } else {
                // Add regular file
                files.push((path, false));
            }
        }
    }

    Ok(())
}

impl FileSystem {
    /// Constructs a file system storage located at `root`
    /// # Errors
    /// Returns an `Err` if current working directory is invalid or `root` doesn't exist
    pub fn new(root: impl AsRef<Path>) -> Result<Self, io::Error> {
        let root_path = root.as_ref();

        // Check if the path exists
        if !root_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Root path does not exist: {}", root_path.display()),
            ));
        }

        // Try to canonicalize, but if it fails on WASM, use the path as-is
        let root = match root_path.canonicalize() {
            Ok(canonical) => canonical,
            Err(e) => {
                if cfg!(target_family = "wasm") || e.kind() == io::ErrorKind::Unsupported {
                    // In WASM or if canonicalization is unsupported, use the path as-is
                    root_path.to_path_buf()
                } else {
                    // For other errors, propagate them
                    return Err(e);
                }
            }
        };

        debug!("File system root = {:?}", root);
        Ok(Self { root })
    }

    /// resolve object path under the virtual root
    fn get_object_path(&self, bucket: &str, key: &str) -> io::Result<PathBuf> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        let ans = dir.join(file_path).absolutize_virtually(&self.root)?.into();
        Ok(ans)
    }

    /// resolve bucket path under the virtual root
    fn get_bucket_path(&self, bucket: &str) -> io::Result<PathBuf> {
        let dir = Path::new(&bucket);
        let ans = dir.absolutize_virtually(&self.root)?.into();
        Ok(ans)
    }

    /// resolve metadata path under the virtual root (custom format)
    fn get_metadata_path(&self, bucket: &str, key: &str) -> io::Result<PathBuf> {
        let encode = |s: &str| base64_simd::URL_SAFE_NO_PAD.encode_to_string(s);

        let file_path_str = format!(
            ".bucket-{}.object-{}.metadata.json",
            encode(bucket),
            encode(key),
        );
        let file_path = Path::new(&file_path_str);
        let ans = file_path.absolutize_virtually(&self.root)?.into();
        Ok(ans)
    }

    /// load metadata from fs
    async fn load_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> io::Result<Option<HashMap<String, String>>> {
        let path = self.get_metadata_path(bucket, key)?;
        if path.exists() {
            let content = fs::read(&path).await?;
            let map = serde_json::from_slice(&content)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(map))
        } else {
            Ok(None)
        }
    }

    /// save metadata
    async fn save_metadata(
        &self,
        bucket: &str,
        key: &str,
        metadata: &HashMap<String, String>,
    ) -> io::Result<()> {
        let path = self.get_metadata_path(bucket, key)?;
        let content = serde_json::to_vec(metadata)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(&path, &content).await
    }

    /// get md5 sum
    async fn get_md5_sum(&self, bucket: &str, key: &str) -> io::Result<String> {
        let object_path = self.get_object_path(bucket, key)?;
        let mut file = File::open(&object_path).await?;
        let mut buf = vec![0; 4_usize.wrapping_mul(1024).wrapping_mul(1024)];
        let mut md5_hash = Md5::new();
        loop {
            let nread = file.read(&mut buf).await?;
            if nread == 0 {
                break;
            }
            md5_hash.update(buf.get(..nread).unwrap_or_else(|| {
                panic!(
                    "nread is larger than buffer size: nread = {}, size = {}",
                    nread,
                    buf.len()
                )
            }));
        }
        md5_hash.finalize().apply(crypto::to_hex_string).apply(Ok)
    }
}

/// copy bytes from a stream to a writer
async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> io::Result<usize>
where
    S: Stream<Item = io::Result<Bytes>> + Send + Unpin,
    W: AsyncWrite + Send + Unpin,
{
    let mut nwrite: usize = 0;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;

        let amt_u64 = tokio::io::copy_buf(&mut bytes.as_ref(), writer).await?;
        let amt: usize = amt_u64.try_into().unwrap_or_else(|err| {
            panic!(
                "number overflow: u64 to usize, n = {}, err = {}",
                amt_u64, err
            )
        });

        assert_eq!(
            bytes.len(),
            amt,
            "amt mismatch: bytes.len() = {}, amt = {}, nwrite = {}",
            bytes.len(),
            amt,
            nwrite
        );

        nwrite = nwrite
            .checked_add(amt)
            .unwrap_or_else(|| panic!("nwrite overflow: amt = {}, nwrite = {}", amt, nwrite));
    }
    writer.flush().await?;
    Ok(nwrite)
}

/// wrap operation error
const fn operation_error<E>(e: E) -> S3StorageError<E> {
    S3StorageError::Operation(e)
}

#[async_trait]
impl S3Storage for FileSystem {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        input: CreateBucketRequest,
    ) -> S3StorageResult<CreateBucketOutput, CreateBucketError> {
        let path = trace_try!(self.get_bucket_path(&input.bucket));

        if path.exists() {
            let err = CreateBucketError::BucketAlreadyExists(String::from(
                "The requested bucket name is not available. \
                    The bucket namespace is shared by all users of the system. \
                    Please select a different name and try again.",
            ));
            return Err(operation_error(err));
        }

        trace_try!(fs::create_dir(&path).await);

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(output)
    }

    #[tracing::instrument]
    async fn copy_object(
        &self,
        input: CopyObjectRequest,
    ) -> S3StorageResult<CopyObjectOutput, CopyObjectError> {
        let copy_source = AmzCopySource::from_header_str(&input.copy_source)
            .map_err(|err| invalid_request!("Invalid header: x-amz-copy-source", err))?;

        let (bucket, key) = match copy_source {
            AmzCopySource::AccessPoint { .. } => {
                return Err(not_supported!("Access point is not supported yet.").into())
            }
            AmzCopySource::Bucket { bucket, key } => (bucket, key),
        };

        let src_path = trace_try!(self.get_object_path(bucket, key));
        let dst_path = trace_try!(self.get_object_path(&input.bucket, &input.key));

        let file_metadata = trace_try!(fs::metadata(&src_path).await);
        let last_modified = time::to_rfc3339(trace_try!(file_metadata.modified()));

        let _ = trace_try!(fs::copy(&src_path, &dst_path).await);

        debug!(
            from = %src_path.display(),
            to = %dst_path.display(),
            "CopyObject: copy file",
        );

        let src_metadata_path = trace_try!(self.get_metadata_path(bucket, key));
        if src_metadata_path.exists() {
            let dst_metadata_path = trace_try!(self.get_metadata_path(&input.bucket, &input.key));
            let _ = trace_try!(fs::copy(src_metadata_path, dst_metadata_path).await);
        }

        let md5_sum = trace_try!(self.get_md5_sum(bucket, key).await);

        let output = CopyObjectOutput {
            copy_object_result: CopyObjectResult {
                e_tag: Some(format!("\"{}\"", md5_sum)),
                last_modified: Some(last_modified),
            }
            .apply(Some),
            ..CopyObjectOutput::default()
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn delete_bucket(
        &self,
        input: DeleteBucketRequest,
    ) -> S3StorageResult<DeleteBucketOutput, DeleteBucketError> {
        let path = trace_try!(self.get_bucket_path(&input.bucket));
        trace_try!(fs::remove_dir_all(path).await);
        Ok(DeleteBucketOutput)
    }

    #[tracing::instrument]
    async fn delete_object(
        &self,
        input: DeleteObjectRequest,
    ) -> S3StorageResult<DeleteObjectOutput, DeleteObjectError> {
        let path = trace_try!(self.get_object_path(&input.bucket, &input.key));
        if input.key.ends_with('/') {
            let mut dir = trace_try!(fs::read_dir(&path).await);
            let is_empty = dir.next_entry().await.ok().flatten().is_none();
            if is_empty {
                trace_try!(fs::remove_dir(&path).await);
            }
        } else {
            trace_try!(fs::remove_file(path).await);
        }
        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(output)
    }

    #[tracing::instrument]
    async fn delete_objects(
        &self,
        input: DeleteObjectsRequest,
    ) -> S3StorageResult<DeleteObjectsOutput, DeleteObjectsError> {
        let mut objects: Vec<(PathBuf, String)> = Vec::new();
        for object in input.delete.objects {
            let path = trace_try!(self.get_object_path(&input.bucket, &object.key));
            if path.exists() {
                objects.push((path, object.key));
            }
        }

        let mut deleted: Vec<DeletedObject> = Vec::new();
        for (path, key) in objects {
            trace_try!(fs::remove_file(path).await);
            deleted.push(DeletedObject {
                key: Some(key),
                ..DeletedObject::default()
            });
        }
        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            ..DeleteObjectsOutput::default()
        };
        Ok(output)
    }

    #[tracing::instrument]
    async fn get_bucket_location(
        &self,
        input: GetBucketLocationRequest,
    ) -> S3StorageResult<GetBucketLocationOutput, GetBucketLocationError> {
        let path = trace_try!(self.get_bucket_path(&input.bucket));

        if !path.exists() {
            let err = code_error!(NoSuchBucket, "NotFound");
            return Err(err.into());
        }

        let output = GetBucketLocationOutput {
            location_constraint: None, // TODO: handle region
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn get_object(
        &self,
        input: GetObjectRequest,
    ) -> S3StorageResult<GetObjectOutput, GetObjectError> {
        let object_path = trace_try!(self.get_object_path(&input.bucket, &input.key));

        let parse_range = |s: &str| {
            Range::from_header_str(s).map_err(|err| invalid_request!("Invalid header: range", err))
        };
        let range: Option<Range> = input.range.as_deref().map(parse_range).transpose()?;

        let mut file = match File::open(&object_path).await {
            Ok(file) => file,
            Err(e) => {
                error!(error = %e, "GetObject: open file");
                let err = code_error!(NoSuchKey, "The specified key does not exist.");
                return Err(err.into());
            }
        };

        let file_metadata = trace_try!(file.metadata().await);
        let last_modified = time::to_rfc3339(trace_try!(file_metadata.modified()));

        let content_length = {
            let file_len = file_metadata.len();
            let content_len = match range {
                None => file_len,
                Some(Range::Normal { first, last }) => {
                    if first >= file_len {
                        let err =
                            code_error!(InvalidRange, "The requested range cannot be satisfied.");
                        return Err(err.into());
                    }
                    let _ = trace_try!(file.seek(SeekFrom::Start(first)).await);

                    // HTTP byte range is inclusive
                    //      len = last + 1 - first
                    // or   len = file_len - first

                    last.and_then(|x| x.checked_add(1))
                        .unwrap_or(file_len)
                        .wrapping_sub(first)
                }
                Some(Range::Suffix { last }) => {
                    let offset = Some(last)
                        .filter(|&x| x <= file_len)
                        .and_then(|x| i64::try_from(x).ok())
                        .and_then(i64::checked_neg);

                    if let Some(x) = offset {
                        let _ = trace_try!(file.seek(SeekFrom::End(x)).await);
                    } else {
                        let err =
                            code_error!(InvalidRange, "The requested range cannot be satisfied.");
                        return Err(err.into());
                    }
                    last
                }
            };
            trace_try!(usize::try_from(content_len))
        };

        let stream = BytesStream::new(file, 4096, Some(content_length));

        let object_metadata = trace_try!(self.load_metadata(&input.bucket, &input.key).await);

        let (md5_sum, duration) = {
            let (ret, duration) =
                time::count_duration(self.get_md5_sum(&input.bucket, &input.key)).await;
            let md5_sum = trace_try!(ret);
            (md5_sum, duration)
        };

        debug!(
            sum = ?md5_sum,
            path = %object_path.display(),
            size = ?content_length,
            ?duration,
            "GetObject: calculate md5 sum",
        );

        let output: GetObjectOutput = GetObjectOutput {
            body: Some(crate::dto::ByteStream::new(stream)),
            content_length: Some(trace_try!(content_length.try_into())),
            last_modified: Some(last_modified),
            metadata: object_metadata,
            e_tag: Some(format!("\"{}\"", md5_sum)),
            ..GetObjectOutput::default() // TODO: handle other fields
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn head_bucket(
        &self,
        input: HeadBucketRequest,
    ) -> S3StorageResult<HeadBucketOutput, HeadBucketError> {
        let path = trace_try!(self.get_bucket_path(&input.bucket));

        if !path.exists() {
            let err = code_error!(NoSuchBucket, "The specified bucket does not exist.");
            return Err(err.into());
        }

        Ok(HeadBucketOutput)
    }

    #[tracing::instrument]
    async fn head_object(
        &self,
        input: HeadObjectRequest,
    ) -> S3StorageResult<HeadObjectOutput, HeadObjectError> {
        let path = trace_try!(self.get_object_path(&input.bucket, &input.key));

        if !path.exists() {
            let err = code_error!(NoSuchKey, "The specified key does not exist.");
            return Err(err.into());
        }

        let file_metadata = trace_try!(fs::metadata(path).await);
        let last_modified = time::to_rfc3339(trace_try!(file_metadata.modified()));
        let size = file_metadata.len();

        let object_metadata = trace_try!(self.load_metadata(&input.bucket, &input.key).await);

        let output: HeadObjectOutput = HeadObjectOutput {
            content_length: Some(trace_try!(size.try_into())),
            content_type: Some(mime::APPLICATION_OCTET_STREAM.as_ref().to_owned()), // TODO: handle content type
            last_modified: Some(last_modified),
            metadata: object_metadata,
            ..HeadObjectOutput::default()
        };
        Ok(output)
    }

    #[tracing::instrument]
    async fn list_buckets(
        &self,
        _: ListBucketsRequest,
    ) -> S3StorageResult<ListBucketsOutput, ListBucketsError> {
        let mut buckets = Vec::new();

        let mut iter = trace_try!(fs::read_dir(&self.root).await);
        loop {
            let entry = trace_try!(iter.next_entry().await);
            if let Some(entry) = entry {
                let file_type = trace_try!(entry.file_type().await);
                if file_type.is_dir() {
                    let file_name = entry.file_name();
                    let name = file_name.to_string_lossy();
                    if S3Path::check_bucket_name(&name) {
                        let file_meta = trace_try!(entry.metadata().await);
                        let creation_date = trace_try!(file_meta.created());
                        buckets.push(Bucket {
                            creation_date: Some(time::to_rfc3339(creation_date)),
                            name: Some(name.into()),
                        });
                    }
                }
            } else {
                break;
            }
        }

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None, // TODO: handle owner
        };
        Ok(output)
    }

    #[tracing::instrument]
    async fn list_objects(
        &self,
        input: ListObjectsRequest,
    ) -> S3StorageResult<ListObjectsOutput, ListObjectsError> {
        let path = trace_try!(self.get_bucket_path(&input.bucket));

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let delimiter_is_dir = input.delimiter.as_deref() == Some("/");
        let request_prefix = input.prefix.as_deref().unwrap_or("");

        // This will be true if we're looking at contents of a specific directory
        let is_dir_listing = request_prefix.ends_with('/');

        // First, collect all files recursively
        let mut all_files: Vec<(PathBuf, bool)> = Vec::new(); // (path, is_dir)
        collect_files_recursive(&path, &mut all_files)
            .await
            .map_err(|e| {
                S3StorageError::Operation(ListObjectsError::NoSuchBucket(e.to_string()))
            })?;

        // For each file, see if it should be included in the listing
        for (file_path, is_dir) in all_files {
            let relative_path = trace_try!(file_path.strip_prefix(&path));
            let key_str = relative_path.to_string_lossy();

            // Skip if it doesn't match the prefix
            if !key_str.starts_with(request_prefix) {
                continue;
            }

            if is_dir {
                let dir_key = format!("{}/", key_str);

                if delimiter_is_dir {
                    // Special handling for delimiter with directories

                    // Check if this is the directory we're listing
                    let is_requested_dir = is_dir_listing && dir_key == request_prefix;

                    // Check if this is a direct child of the directory we're listing
                    let is_direct_child = if is_dir_listing {
                        let prefix_parts =
                            request_prefix.split('/').filter(|p| !p.is_empty()).count();
                        let dir_parts = dir_key.split('/').filter(|p| !p.is_empty()).count();
                        dir_key.starts_with(request_prefix) && dir_parts == prefix_parts + 1
                    } else {
                        let parts = dir_key.split('/').filter(|p| !p.is_empty()).count();
                        parts == 1
                    };

                    if !is_requested_dir && is_direct_child {
                        // Add direct child directories as common prefixes
                        common_prefixes.push(CommonPrefix {
                            prefix: Some(dir_key),
                        });
                    }
                }
            } else {
                let metadata = trace_try!(file_path.metadata());
                let last_modified = time::to_rfc3339(trace_try!(metadata.modified()));
                let size = metadata.len();

                if delimiter_is_dir {
                    // With delimiter, we need to handle file paths carefully

                    // Check if file is in a subdirectory
                    if key_str.contains('/') {
                        if is_dir_listing {
                            // In a directory listing, include only direct children
                            let remaining_path = key_str
                                .strip_prefix(request_prefix)
                                .unwrap_or(key_str.as_ref());
                            if !remaining_path.contains('/') {
                                // Direct child of the requested directory
                                objects.push(Object {
                                    e_tag: None,
                                    key: Some(key_str.into_owned()),
                                    last_modified: Some(last_modified),
                                    owner: None,
                                    size: Some(trace_try!(size.try_into())),
                                    storage_class: None,
                                });
                            }
                        } else {
                            // Root listing, group files by their top-level directory
                            let top_dir = key_str.split('/').next().unwrap_or("");
                            let prefix = format!("{}/", top_dir);

                            if !common_prefixes
                                .iter()
                                .any(|cp| cp.prefix.as_deref() == Some(&prefix))
                            {
                                common_prefixes.push(CommonPrefix {
                                    prefix: Some(prefix),
                                });
                            }
                        }
                    } else {
                        // File in the root level
                        objects.push(Object {
                            e_tag: None,
                            key: Some(key_str.into_owned()),
                            last_modified: Some(last_modified),
                            owner: None,
                            size: Some(trace_try!(size.try_into())),
                            storage_class: None,
                        });
                    }
                } else {
                    // Without delimiter, include all files
                    objects.push(Object {
                        e_tag: None,
                        key: Some(key_str.into_owned()),
                        last_modified: Some(last_modified),
                        owner: None,
                        size: Some(trace_try!(size.try_into())),
                        storage_class: None,
                    });
                }
            }
        }

        objects.sort_by(|lhs, rhs| {
            let lhs_key = lhs.key.as_deref().unwrap_or("");
            let rhs_key = rhs.key.as_deref().unwrap_or("");
            lhs_key.cmp(rhs_key)
        });

        // TODO: handle other fields
        let output = ListObjectsOutput {
            contents: Some(objects),
            delimiter: input.delimiter,
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            common_prefixes: if common_prefixes.is_empty() {
                None
            } else {
                Some(common_prefixes)
            },
            is_truncated: None,
            marker: None,
            max_keys: None,
            next_marker: None,
            prefix: None,
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        input: ListObjectsV2Request,
    ) -> S3StorageResult<ListObjectsV2Output, ListObjectsV2Error> {
        let path = trace_try!(self.get_bucket_path(&input.bucket));

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let delimiter_is_dir = input.delimiter.as_deref() == Some("/");
        let request_prefix = input.prefix.as_deref().unwrap_or("");

        // This will be true if we're looking at contents of a specific directory
        let is_dir_listing = request_prefix.ends_with('/');

        // First, collect all files recursively
        let mut all_files: Vec<(PathBuf, bool)> = Vec::new(); // (path, is_dir)
        collect_files_recursive(&path, &mut all_files)
            .await
            .map_err(|e| {
                S3StorageError::Operation(ListObjectsV2Error::NoSuchBucket(e.to_string()))
            })?;

        // For each file, see if it should be included in the listing
        for (file_path, is_dir) in all_files {
            let relative_path = trace_try!(file_path.strip_prefix(&path));
            let key_str = relative_path.to_string_lossy();

            // Skip if it doesn't match the prefix
            if !key_str.starts_with(request_prefix) {
                continue;
            }

            if is_dir {
                let dir_key = format!("{}/", key_str);

                if delimiter_is_dir {
                    // Special handling for delimiter with directories

                    // Check if this is the directory we're listing
                    let is_requested_dir = is_dir_listing && dir_key == request_prefix;

                    // Check if this is a direct child of the directory we're listing
                    let is_direct_child = if is_dir_listing {
                        let prefix_parts =
                            request_prefix.split('/').filter(|p| !p.is_empty()).count();
                        let dir_parts = dir_key.split('/').filter(|p| !p.is_empty()).count();
                        dir_key.starts_with(request_prefix) && dir_parts == prefix_parts + 1
                    } else {
                        let parts = dir_key.split('/').filter(|p| !p.is_empty()).count();
                        parts == 1
                    };

                    if !is_requested_dir && is_direct_child {
                        // Add direct child directories as common prefixes
                        common_prefixes.push(CommonPrefix {
                            prefix: Some(dir_key),
                        });
                    }
                }
            } else {
                let metadata = trace_try!(file_path.metadata());
                let last_modified = time::to_rfc3339(trace_try!(metadata.modified()));
                let size = metadata.len();

                if delimiter_is_dir {
                    // With delimiter, we need to handle file paths carefully

                    // Check if file is in a subdirectory
                    if key_str.contains('/') {
                        if is_dir_listing {
                            // In a directory listing, include only direct children
                            let remaining_path = key_str
                                .strip_prefix(request_prefix)
                                .unwrap_or(key_str.as_ref());
                            if !remaining_path.contains('/') {
                                // Direct child of the requested directory
                                objects.push(Object {
                                    e_tag: None,
                                    key: Some(key_str.into_owned()),
                                    last_modified: Some(last_modified),
                                    owner: None,
                                    size: Some(trace_try!(size.try_into())),
                                    storage_class: None,
                                });
                            }
                        } else {
                            // Root listing, group files by their top-level directory
                            let top_dir = key_str.split('/').next().unwrap_or("");
                            let prefix = format!("{}/", top_dir);

                            if !common_prefixes
                                .iter()
                                .any(|cp| cp.prefix.as_deref() == Some(&prefix))
                            {
                                common_prefixes.push(CommonPrefix {
                                    prefix: Some(prefix),
                                });
                            }
                        }
                    } else {
                        // File in the root level
                        objects.push(Object {
                            e_tag: None,
                            key: Some(key_str.into_owned()),
                            last_modified: Some(last_modified),
                            owner: None,
                            size: Some(trace_try!(size.try_into())),
                            storage_class: None,
                        });
                    }
                } else {
                    // Without delimiter, include all files
                    objects.push(Object {
                        e_tag: None,
                        key: Some(key_str.into_owned()),
                        last_modified: Some(last_modified),
                        owner: None,
                        size: Some(trace_try!(size.try_into())),
                        storage_class: None,
                    });
                }
            }
        }

        objects.sort_by(|lhs, rhs| {
            let lhs_key = lhs.key.as_deref().unwrap_or("");
            let rhs_key = rhs.key.as_deref().unwrap_or("");
            lhs_key.cmp(rhs_key)
        });

        // TODO: handle other fields
        let output = ListObjectsV2Output {
            key_count: Some(trace_try!(objects.len().try_into())),
            contents: Some(objects),
            delimiter: input.delimiter,
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            common_prefixes: if common_prefixes.is_empty() {
                None
            } else {
                Some(common_prefixes)
            },
            is_truncated: None,
            max_keys: None,
            prefix: None,
            continuation_token: None,
            next_continuation_token: None,
            start_after: None,
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn put_object(
        &self,
        input: PutObjectRequest,
    ) -> S3StorageResult<PutObjectOutput, PutObjectError> {
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                let err = code_error!(
                    InvalidStorageClass,
                    "The storage class you specified is not valid."
                );
                return Err(err.into());
            }
        }

        let PutObjectRequest {
            body,
            bucket,
            key,
            metadata,
            content_length,
            ..
        } = input;

        let body = body.ok_or_else(||{
            code_error!(IncompleteBody,"You did not provide the number of bytes specified by the Content-Length HTTP header.")
        })?;

        if key.ends_with('/') {
            if content_length == Some(0) {
                let object_path = trace_try!(self.get_object_path(&bucket, &key));
                trace_try!(fs::create_dir_all(&object_path).await);
                let output = PutObjectOutput::default();
                return Ok(output);
            }
            let err = code_error!(
                UnexpectedContent,
                "Unexpected request body when creating a directory object."
            );
            return Err(err.into());
        }

        let object_path = trace_try!(self.get_object_path(&bucket, &key));
        if let Some(dir_path) = object_path.parent() {
            trace_try!(fs::create_dir_all(&dir_path).await);
        }

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));

        let file = trace_try!(File::create(&object_path).await);
        let mut writer = BufWriter::new(file);

        let (ret, duration) = time::count_duration(copy_bytes(stream, &mut writer)).await;
        let size = trace_try!(ret);
        let md5_sum = md5_hash.finalize().apply(crypto::to_hex_string);

        debug!(
            path = %object_path.display(),
            ?size,
            ?duration,
            %md5_sum,
            "PutObject: write file",
        );

        if let Some(ref metadata) = metadata {
            trace_try!(self.save_metadata(&bucket, &key, metadata).await);
        }

        let output = PutObjectOutput {
            e_tag: Some(format!("\"{}\"", md5_sum)),
            ..PutObjectOutput::default()
        }; // TODO: handle other fields

        Ok(output)
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        input: CreateMultipartUploadRequest,
    ) -> S3StorageResult<CreateMultipartUploadOutput, CreateMultipartUploadError> {
        let upload_id = Uuid::new_v4().to_string();

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id),
            ..CreateMultipartUploadOutput::default()
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn upload_part(
        &self,
        input: UploadPartRequest,
    ) -> S3StorageResult<UploadPartOutput, UploadPartError> {
        let UploadPartRequest {
            body,
            upload_id,
            part_number,
            ..
        } = input;

        let body = body.ok_or_else(||{
            code_error!(IncompleteBody, "You did not provide the number of bytes specified by the Content-Length HTTP header.")
        })?;

        let file_path_str = format!(".upload_id-{}.part-{}", upload_id, part_number);
        let file_path = trace_try!(Path::new(&file_path_str).absolutize_virtually(&self.root));

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));

        let file = trace_try!(File::create(&file_path).await);
        let mut writer = BufWriter::new(file);

        let (ret, duration) = time::count_duration(copy_bytes(stream, &mut writer)).await;
        let size = trace_try!(ret);
        let md5_sum = md5_hash.finalize().apply(crypto::to_hex_string);

        debug!(
            path = %file_path.display(),
            ?size,
            ?duration,
            %md5_sum,
            "UploadPart: write file",
        );

        let e_tag = format!("\"{}\"", md5_sum);

        let output = UploadPartOutput {
            e_tag: Some(e_tag),
            ..UploadPartOutput::default()
        };

        Ok(output)
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        input: CompleteMultipartUploadRequest,
    ) -> S3StorageResult<CompleteMultipartUploadOutput, CompleteMultipartUploadError> {
        let CompleteMultipartUploadRequest {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = input;

        let multipart_upload = if let Some(multipart_upload) = multipart_upload {
            multipart_upload
        } else {
            let err = code_error!(InvalidPart, "Missing multipart_upload");
            return Err(err.into());
        };

        let object_path = trace_try!(self.get_object_path(&bucket, &key));
        let file = trace_try!(File::create(&object_path).await);
        let mut writer = BufWriter::new(file);

        let mut cnt: i64 = 0;
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = trace_try!(part
                .part_number
                .ok_or_else(|| { io::Error::new(io::ErrorKind::NotFound, "Missing part_number") }));
            cnt = cnt.wrapping_add(1);
            if part_number != cnt {
                trace_try!(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "InvalidPartOrder"
                )));
            }
            let part_path_str = format!(".upload_id-{}.part-{}", upload_id, part_number);
            let part_path = trace_try!(Path::new(&part_path_str).absolutize_virtually(&self.root));

            let mut reader = trace_try!(File::open(&part_path).await);
            let (ret, duration) =
                time::count_duration(tokio::io::copy(&mut reader, &mut writer)).await;
            let size = trace_try!(ret);

            debug!(
                from = %part_path.display(),
                to = %object_path.display(),
                ?size,
                ?duration,
                "CompleteMultipartUpload: write file",
            );
            trace_try!(fs::remove_file(&part_path).await);
        }
        drop(writer);

        let file_size = trace_try!(fs::metadata(&object_path).await).len();

        let (md5_sum, duration) = {
            let (ret, duration) = time::count_duration(self.get_md5_sum(&bucket, &key)).await;
            let md5_sum = trace_try!(ret);
            (md5_sum, duration)
        };

        debug!(
            sum = ?md5_sum,
            path = %object_path.display(),
            size = ?file_size,
            ?duration,
            "CompleteMultipartUpload: calculate md5 sum",
        );

        let e_tag = format!("\"{}\"", md5_sum);
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(e_tag),
            ..CompleteMultipartUploadOutput::default()
        };
        Ok(output)
    }
}
