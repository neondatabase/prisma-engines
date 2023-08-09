pub mod graphql;
pub mod json;

use query_core::{protocol::EngineProtocol, schema::QuerySchemaRef, QueryDocument};

#[derive(Debug)]
pub enum RequestBody {
    Graphql(graphql::GraphqlBody),
    Json(json::JsonBody),
}

impl RequestBody {
    pub fn into_doc(self, query_schema: &QuerySchemaRef) -> crate::Result<QueryDocument> {
        match self {
            RequestBody::Graphql(body) => {
                #[cfg(feature = "graphql")]
                {
                    body.into_doc()
                }
                #[cfg(not(feature = "graphql"))]
                {
                    panic!("graphql feature is not enabled")
                }
            }
            RequestBody::Json(body) => body.into_doc(query_schema),
        }
    }

    pub fn try_from_str(val: &str, engine_protocol: EngineProtocol) -> Result<RequestBody, serde_json::Error> {
        match engine_protocol {
            EngineProtocol::Graphql => {
                #[cfg(feature = "graphql")]
                {
                    serde_json::from_str::<graphql::GraphqlBody>(val).map(Self::from)
                }
                #[cfg(not(feature = "graphql"))]
                {
                    panic!("graphql feature is not enabled")
                }
            }
            EngineProtocol::Json => serde_json::from_str::<json::JsonBody>(val).map(Self::from),
        }
    }

    #[cfg(feature = "js-connectors")]
    pub fn try_from_js_value(val: wasm_bindgen::prelude::JsValue) -> Result<RequestBody, serde_json::Error> {
        Ok(serde_wasm_bindgen::from_value::<json::JsonBody>(val).unwrap().into())
    }

    pub fn try_from_slice(val: &[u8], engine_protocol: EngineProtocol) -> Result<RequestBody, serde_json::Error> {
        match engine_protocol {
            EngineProtocol::Graphql => {
                #[cfg(feature = "graphql")]
                {
                    serde_json::from_slice::<graphql::GraphqlBody>(val).map(Self::from)
                }
                #[cfg(not(feature = "graphql"))]
                {
                    panic!("graphql feature is not enabled")
                }
            }
            EngineProtocol::Json => serde_json::from_slice::<json::JsonBody>(val).map(Self::from),
        }
    }
}

#[cfg(feature = "graphql")]
impl From<graphql::GraphqlBody> for RequestBody {
    fn from(body: graphql::GraphqlBody) -> Self {
        Self::Graphql(body)
    }
}

impl From<json::JsonBody> for RequestBody {
    fn from(body: json::JsonBody) -> Self {
        Self::Json(body)
    }
}
