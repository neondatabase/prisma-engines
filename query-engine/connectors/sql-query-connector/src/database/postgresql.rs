#![allow(implied_bounds_entailment)]

use super::connection::SqlConnection;
use crate::SqlError;
use async_trait::async_trait;
use connector_interface::{
    error::{ConnectorError, ErrorKind},
    Connection, Connector,
};
use psl::builtin_connectors::COCKROACH;
use quaint::{
    connector::IsolationLevel,
    prelude::{ConnectionInfo, Queryable, TransactionCapable},
    Value,
};
use std::{marker::PhantomData, time::Duration};
