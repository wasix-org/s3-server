#[macro_use]
mod utils;

use self::utils::{fs_write_object, generate_path, parse_mime, recv_body_string};
use self::utils::{Request, ResultExt};

use s3_server::headers::X_AMZ_CONTENT_SHA256;
use s3_server::path::S3Path;
use s3_server::storages::fs::FileSystem;
use s3_server::S3Service;

use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Body, Method, StatusCode};
use tracing::{debug_span, error};
use uuid::Uuid;

macro_rules! enter_sync {
    ($span:expr) => {
        let __span = $span;
        let __enter = __span.enter();
    };
}

pub fn setup_tracing() {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::fmt::time::UtcTime;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    tracing_subscriber::fmt()
        .event_format(fmt::format::Format::default().pretty())
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(UtcTime::rfc_3339())
        .finish()
        .with(ErrorLayer::default())
        .try_init()
        .ok();
}

fn setup_fs_root(clear: bool) -> Result<PathBuf> {
    let root: PathBuf = env::var("S3_TEST_FS_ROOT")
        .unwrap_or_else(|_| "target/s3-test".into())
        .into();

    enter_sync!(debug_span!("setup fs root", ?clear, root = %root.display()));

    // Create a unique test directory for each test run to avoid conflicts
    let test_id = Uuid::new_v4().to_string();
    let root = root.join(test_id);

    // Create fresh directory
    if root.exists() && clear {
        // Try multiple times with backoff to handle potential locks or timing issues
        let mut retry_count = 0;
        while retry_count < 3 {
            match fs::remove_dir_all(&root) {
                Ok(_) => break,
                Err(e) if retry_count < 2 => {
                    error!(%e, retry = retry_count, "failed to remove root directory, retrying");
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    retry_count += 1;
                }
                Err(e) => {
                    // On final retry, ignore the error and try to continue
                    error!(%e, "failed to remove root directory, continuing anyway");
                    break;
                }
            }
        }
    }

    // Always create the directory fresh
    fs::create_dir_all(&root)
        .unstable_inspect_err(|err| error!(%err, "failed to create directory"))?;

    Ok(root)
}

fn setup_service() -> Result<(PathBuf, S3Service)> {
    setup_tracing();

    let root = setup_fs_root(true).unwrap();

    enter_sync!(debug_span!("setup service", root = %root.display()));

    let fs = FileSystem::new(&root)
        .unstable_inspect_err(|err| error!(%err, "failed to create filesystem"))?;

    let service = S3Service::new(fs);

    Ok((root, service))
}

mod success {
    use super::*;

    #[tokio::test]
    async fn get_object() {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let key = "qwe";
        let content = "Hello World!";

        fs_write_object(root, bucket, key, content).unwrap();

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::GET;
        *req.uri_mut() = format!("http://localhost/{}/{}", bucket, key)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body, content);
    }

    #[tokio::test]
    async fn put_object() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let key = "qwe";
        let content = "Hello World!";

        let dir_path = generate_path(&root, S3Path::Bucket { bucket });
        fs::create_dir(dir_path).unwrap();

        let mut req = Request::new(Body::from(content));
        *req.method_mut() = Method::PUT;
        *req.uri_mut() = format!("http://localhost/{}/{}", bucket, key)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body, "");

        let file_path = generate_path(root, S3Path::Object { bucket, key });
        let file_content = fs::read_to_string(file_path).unwrap();

        assert_eq!(file_content, content);

        Ok(())
    }

    #[tokio::test]
    async fn delete_object() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let key = "qwe";
        let content = "Hello World!";

        fs_write_object(&root, bucket, key, content).unwrap();

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::DELETE;
        *req.uri_mut() = format!("http://localhost/{}/{}", bucket, key)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(body, "");

        let file_path = generate_path(&root, S3Path::Object { bucket, key });
        assert!(!file_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn create_bucket() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let dir_path = generate_path(root, S3Path::Bucket { bucket });

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::PUT;
        *req.uri_mut() = format!("http://localhost/{}", bucket).parse().unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body, "");

        assert!(dir_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn delete_bucket() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let dir_path = generate_path(root, S3Path::Bucket { bucket });
        fs::create_dir(&dir_path).unwrap();

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::DELETE;
        *req.uri_mut() = format!("http://localhost/{}", bucket).parse().unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(body, "");

        assert!(!dir_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn head_bucket() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let dir_path = generate_path(root, S3Path::Bucket { bucket });
        fs::create_dir(&dir_path).unwrap();

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::HEAD;
        *req.uri_mut() = format!("http://localhost/{}", bucket).parse().unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body, "");

        Ok(())
    }

    #[tokio::test]
    async fn list_bucket() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let buckets = ["asd", "qwe"];
        for &bucket in buckets.iter() {
            let dir_path = generate_path(&root, S3Path::Bucket { bucket });
            fs::create_dir(&dir_path).unwrap();
        }

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::GET;
        *req.uri_mut() = "http://localhost/".parse().unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let parser = xml::EventReader::new(io::Cursor::new(body.as_bytes()));
        let mut in_name = false;
        for e in parser {
            let e = e.unwrap();
            match e {
                xml::reader::XmlEvent::StartElement { name, .. } => {
                    if name.local_name == "Name" {
                        in_name = true;
                    }
                }
                xml::reader::XmlEvent::EndElement { name } => {
                    if name.local_name == "Name" {
                        in_name = false;
                    }
                }
                xml::reader::XmlEvent::Characters(s) => {
                    if in_name {
                        assert!(["asd", "qwe"].contains(&s.as_str()));
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn put_object_metadata_in_reserved_dir() -> Result<()> {
        let (root, service) = setup_service().unwrap();
        let bucket = "asd";
        let key = "dir/obj";
        let content = "hi";
        fs::create_dir(generate_path(&root, S3Path::Bucket { bucket })).unwrap();

        let mut req = Request::new(Body::from(content));
        *req.method_mut() = Method::PUT;
        *req.uri_mut() = format!("http://localhost/{}/{}", bucket, key)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );
        req.headers_mut().insert(
            HeaderName::from_static("x-amz-meta-foo"),
            HeaderValue::from_static("bar"),
        );

        assert_eq!(
            service.hyper_call(req).await.unwrap().status(),
            StatusCode::OK
        );

        // object stored at its real path
        let obj = generate_path(&root, S3Path::Object { bucket, key });
        assert_eq!(fs::read_to_string(obj).unwrap(), content);

        // metadata sidecar lives in the bucket's reserved dir, nothing at the fs-root
        let meta_dir = root.join(bucket).join(".wasmer-s3").join("meta");
        assert_eq!(fs::read_dir(&meta_dir).unwrap().count(), 1);
        let root_has_sidecar = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".metadata.json"));
        assert!(!root_has_sidecar);

        Ok(())
    }

    #[tokio::test]
    async fn list_omits_reserved_dir() -> Result<()> {
        let (root, service) = setup_service().unwrap();
        let bucket = "asd";
        fs::create_dir(generate_path(&root, S3Path::Bucket { bucket })).unwrap();

        let mut put = Request::new(Body::from("x"));
        *put.method_mut() = Method::PUT;
        *put.uri_mut() = format!("http://localhost/{}/obj", bucket).parse().unwrap();
        put.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );
        put.headers_mut().insert(
            HeaderName::from_static("x-amz-meta-foo"),
            HeaderValue::from_static("bar"),
        );
        assert_eq!(
            service.hyper_call(put).await.unwrap().status(),
            StatusCode::OK
        );
        assert!(root.join(bucket).join(".wasmer-s3").exists());

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::GET;
        *req.uri_mut() = format!("http://localhost/{}?list-type=2", bucket)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body.contains("obj"));
        assert!(!body.contains(".wasmer-s3"));

        Ok(())
    }
}

mod error {
    use super::*;

    #[tokio::test]
    async fn get_object() {
        let (_, service) = setup_service().unwrap();

        let bucket = "asd";
        let key = "qwe";

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::GET;
        *req.uri_mut() = format!("http://localhost/{}/{}", bucket, key)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();
        let mime = parse_mime(&res).unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(mime, mime::TEXT_XML);
        assert_eq!(
            body,
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<Error>",
                "<Code>NoSuchKey</Code>",
                "<Message>The specified key does not exist.</Message>",
                "</Error>"
            )
        );
    }

    #[tokio::test]
    async fn head_bucket() -> Result<()> {
        let (_, service) = setup_service().unwrap();

        let bucket = "asd";

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::HEAD;
        *req.uri_mut() = format!("http://localhost/{}", bucket).parse().unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();
        let mime = parse_mime(&res).unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(mime, mime::TEXT_XML);
        assert_eq!(
            body,
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<Error>",
                "<Code>NoSuchBucket</Code>",
                "<Message>The specified bucket does not exist.</Message>",
                "</Error>"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_bucket() -> Result<()> {
        let (root, service) = setup_service().unwrap();

        let bucket = "asd";
        let dir_path = generate_path(root, S3Path::Bucket { bucket });
        fs::create_dir(dir_path)?;

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::PUT;
        *req.uri_mut() = format!("http://localhost/{}", bucket).parse().unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();
        let mime = parse_mime(&res).unwrap();

        assert_eq!(res.status(), StatusCode::CONFLICT);
        assert_eq!(mime, mime::TEXT_XML);
        assert_eq!(
            body,
            concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<Error>",
                "<Code>BucketAlreadyExists</Code>",
                "<Message>",
                "The requested bucket name is not available. ",
                "The bucket namespace is shared by all users of the system. ",
                "Please select a different name and try again.",
                "</Message>",
                "</Error>"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn put_reserved_key() -> Result<()> {
        let (root, service) = setup_service().unwrap();
        let bucket = "asd";
        fs::create_dir(generate_path(&root, S3Path::Bucket { bucket })).unwrap();

        let mut req = Request::new(Body::from("x"));
        *req.method_mut() = Method::PUT;
        *req.uri_mut() = format!("http://localhost/{}/.wasmer-s3/evil", bucket)
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        assert!(body.contains("AccessDenied"));
        assert!(body.contains("reserved for internal use"));
        assert!(!root.join(bucket).join(".wasmer-s3").join("evil").exists());

        Ok(())
    }

    #[tokio::test]
    async fn get_reserved_key() {
        let (_, service) = setup_service().unwrap();

        let mut req = Request::new(Body::empty());
        *req.method_mut() = Method::GET;
        *req.uri_mut() = "http://localhost/asd/.wasmer-s3/meta/x.json"
            .parse()
            .unwrap();
        req.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut res = service.hyper_call(req).await.unwrap();
        let body = recv_body_string(&mut res).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(body.contains("NoSuchKey"));
    }
}
