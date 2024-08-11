//! [`GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

use super::{ReqContext, S3Handler};

use crate::errors::{S3Error, S3ErrorCode, S3Result};
use crate::storage::S3Storage;
use crate::{async_trait, Method, Response};
use hyper::header::{
    HeaderValue, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN,
};

/// `GetObject` handler
pub struct Handler;

#[async_trait]
impl S3Handler for Handler {
    fn is_match(&self, ctx: &'_ ReqContext<'_>) -> bool {
        ctx.req.method() == Method::OPTIONS
    }

    async fn handle(
        &self,
        _ctx: &mut ReqContext<'_>,
        _storage: &(dyn S3Storage + Send + Sync),
    ) -> S3Result<Response> {
        // handle options call
        let mut res = Response::default();
        let _ = res
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"))
            .ok_or(S3Error::new(
                S3ErrorCode::InternalError,
                "Failed to set header",
            ));
        let _ = res
            .headers_mut()
            .insert(
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, PUT, POST, DELETE, HEAD, OPTIONS"),
            )
            .ok_or(S3Error::new(
                S3ErrorCode::InternalError,
                "Failed to set header",
            ));
        let _ = res
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"))
            .ok_or(S3Error::new(
                S3ErrorCode::InternalError,
                "Failed to set header",
            ));
        Ok(res)
    }
}
