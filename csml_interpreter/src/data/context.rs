use std::{
    fmt::Debug,
    sync::{mpsc, Arc},
};

use crate::{
    data::{
        primitive::{PrimitiveObject, PrimitiveType},
        Client, Hold, Interval, Literal,
    },
    error_format::ErrorInfo,
};

use crate::interpreter::{json_to_literal, memory_to_literal};

use nom::lib::std::collections::HashMap;
use serde::{Deserialize, Serialize};

use super::{ArgsType, Data, MessageData, MSG};

////////////////////////////////////////////////////////////////////////////////
// DATA STRUCTURE
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApiInfo {
    pub client: Client,
    pub apps_endpoint: String,
}

pub trait Extension:
    for<'r> Fn(
        ArgsType,
        Interval,
        &mut Data,
        &mut MessageData,
        &Option<mpsc::Sender<MSG>>,
    ) -> Result<Literal, ErrorInfo>
    + Debug
{
}

#[derive(Debug, Clone)]
pub struct ExtensionInfo {
    pub function_list: Vec<String>,
    pub function_map: HashMap<String, Arc<dyn Extension>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContextStepInfo {
    Normal(String),
    UnknownFlow(String),
    InsertedStep { step: String, flow: String },
}

impl ContextStepInfo {
    pub fn get_step(&self) -> String {
        match self {
            ContextStepInfo::Normal(step)
            | ContextStepInfo::UnknownFlow(step)
            | ContextStepInfo::InsertedStep { step, flow: _ } => step.to_owned(),
        }
    }

    pub fn get_step_ref(&self) -> &str {
        match self {
            ContextStepInfo::Normal(step)
            | ContextStepInfo::UnknownFlow(step)
            | ContextStepInfo::InsertedStep { step, flow: _ } => step,
        }
    }

    pub fn is_step(&self, cmp_step: &str) -> bool {
        match self {
            ContextStepInfo::Normal(step)
            | ContextStepInfo::UnknownFlow(step)
            | ContextStepInfo::InsertedStep { step, flow: _ } => step == cmp_step,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PreviousBot {
    pub bot: String,
    pub flow: String,
    pub step: String,
}

#[derive(Debug, Clone)]
pub struct Context {
    pub current: HashMap<String, Literal>,
    pub metadata: HashMap<String, Literal>,
    pub api_info: Option<ApiInfo>,
    pub extension_info: Option<ExtensionInfo>,
    pub hold: Option<Hold>,
    pub step: ContextStepInfo,
    pub flow: String,
    pub previous_bot: Option<PreviousBot>,
}

////////////////////////////////////////////////////////////////////////////////
// STATIC FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

pub fn get_hashmap_from_mem(lit: &serde_json::Value, flow_name: &str) -> HashMap<String, Literal> {
    match memory_to_literal(
        lit,
        Interval {
            start_line: 0,
            start_column: 0,
            end_line: None,
            end_column: None,
            offset: 0,
        },
        flow_name,
    ) {
        Ok(vars) if vars.primitive.get_type() == PrimitiveType::PrimitiveObject => {
            match vars.primitive.as_any().downcast_ref::<PrimitiveObject>() {
                Some(map) => map.value.clone(),
                None => HashMap::new(),
            }
        }
        _ => HashMap::new(),
    }
}

pub fn get_hashmap_from_json(lit: &serde_json::Value, flow_name: &str) -> HashMap<String, Literal> {
    match json_to_literal(
        lit,
        Interval {
            start_line: 0,
            start_column: 0,
            end_line: None,
            end_column: None,
            offset: 0,
        },
        flow_name,
    ) {
        Ok(vars) if vars.primitive.get_type() == PrimitiveType::PrimitiveObject => {
            match vars.primitive.as_any().downcast_ref::<PrimitiveObject>() {
                Some(map) => map.value.clone(),
                None => HashMap::new(),
            }
        }
        _ => HashMap::new(),
    }
}

impl Context {
    pub fn new(
        current: HashMap<String, Literal>,
        metadata: HashMap<String, Literal>,
        api_info: Option<ApiInfo>,
        hold: Option<Hold>,
        step: &str,
        flow: &str,
        previous_bot: Option<PreviousBot>,
    ) -> Self {
        Self {
            current,
            metadata,
            api_info,
            hold,
            step: ContextStepInfo::Normal(step.to_owned()),
            flow: flow.to_owned(),
            previous_bot,
            extension_info: None,
        }
    }

    pub fn with_extensions(self, extensions: HashMap<String, Arc<dyn Extension>>) -> Self {
        let extension_info = ExtensionInfo {
            function_list: extensions.keys().map(|s| s.to_string()).collect(),
            function_map: extensions,
        };
        Self {
            extension_info: Some(extension_info),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

pub fn get_hashmap(lit: &serde_json::Value, flow_name: &str) -> HashMap<String, Literal> {
    match json_to_literal(
        lit,
        Interval {
            start_line: 0,
            start_column: 0,
            end_line: None,
            end_column: None,
            offset: 0,
        },
        flow_name,
    ) {
        Ok(vars) if vars.primitive.get_type() == PrimitiveType::PrimitiveObject => {
            match vars.primitive.as_any().downcast_ref::<PrimitiveObject>() {
                Some(map) => map.value.clone(),
                None => HashMap::new(),
            }
        }
        _ => HashMap::new(),
    }
}
