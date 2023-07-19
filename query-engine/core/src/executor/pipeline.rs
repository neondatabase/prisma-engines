use crate::{Env, Expressionista, IrSerializer, QueryGraph, QueryInterpreter, ResponseData};
use prisma_models::PrismaValue;
use schema::QuerySchema;
use tracing::Instrument;

#[derive(Debug)]
pub(crate) struct QueryPipeline<'conn, 'schema> {
    graph: QueryGraph,
    interpreter: QueryInterpreter<'conn>,
    serializer: IrSerializer<'schema>,
}

impl<'conn, 'schema> QueryPipeline<'conn, 'schema> {
    pub(crate) fn new(
        graph: QueryGraph,
        interpreter: QueryInterpreter<'conn>,
        serializer: IrSerializer<'schema>,
    ) -> Self {
        Self {
            graph,
            interpreter,
            serializer,
        }
    }

    pub(crate) async fn execute(
        mut self,
        query_schema: &'schema QuerySchema,
        trace_id: Option<String>,
    ) -> crate::Result<ResponseData> {
        let serializer = self.serializer;
        let expr = Expressionista::translate(self.graph)?;

        let span = info_span!("prisma:engine:interpret");

        let result = self
            .interpreter
            .interpret(expr, Env::default(), 0, trace_id)
            .instrument(span)
            .await;

        trace!("{}", self.interpreter.log_output());
        use crate::response_ir::Item;
        Ok(ResponseData::new("empty".to_string(), Item::Value(PrismaValue::Null)))
        // serializer.serialize(result?, query_schema) // TODO: serialize
    }
}
