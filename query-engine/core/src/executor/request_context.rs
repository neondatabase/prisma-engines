use crate::protocol::EngineProtocol;
use prisma_models::PrismaValue;

#[derive(Debug)]
struct RequestContext {
    request_now: PrismaValue,
    engine_protocol: EngineProtocol,
}

/// A timestamp that should be the `NOW()` value for the whole duration of a request. So all
/// `@default(now())` and `@updatedAt` should use it.
///
/// That panics if REQUEST_CONTEXT has not been set with with_request_context().
///
/// If we had a query context we carry for the entire lifetime of the query, it would belong there.
pub(crate) fn get_request_now() -> PrismaValue {
    // FIXME: we want to bypass task locals if this code is executed outside of a tokio context. As
    // of this writing, it happens only in the query validation test suite.
    //
    // Eventually, this will go away when we have a plain query context reference we pass around.
    return PrismaValue::DateTime(chrono::Utc::now().into());
}

/// The engine protocol used for the whole duration of a request.
/// Use with caution to avoid creating implicit and unnecessary dependencies.
///
/// That panics if REQUEST_CONTEXT has not been set with with_request_context().
///
/// If we had a query context we carry for the entire lifetime of the query, it would belong there.
pub(crate) fn get_engine_protocol() -> EngineProtocol {
    EngineProtocol::Graphql
}

/// Execute a future with the current "now" timestamp that can be retrieved through
/// `get_request_now()`, initializing it if necessary.
pub(crate) async fn with_request_context<F, R>(engine_protocol: EngineProtocol, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    fut.await
}
