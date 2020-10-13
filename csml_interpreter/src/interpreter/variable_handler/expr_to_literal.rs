use crate::data::error_info::ErrorInfo;
use crate::data::literal::ContentType;
use crate::data::primitive::{PrimitiveArray, PrimitiveObject};
use crate::data::Position;
use crate::data::{
    ast::*, tokens::*, ArgsType, Context, Data, Literal, MemoryType, MessageData, MSG,
};
use crate::error_format::*;
use crate::interpreter::{
    ast_interpreter::evaluate_condition,
    builtins::{match_builtin, match_native_builtin},
    interpret_function_scope,
    json_to_rust::interpolate,
    variable_handler::{
        exec_path_actions, get_string_from_complex_string, get_var, interval::interval_from_expr,
        resolve_path, save_literal_in_mem,
    },
};
use std::{collections::HashMap, sync::mpsc};

use crate::search_function;

fn exec_path_literal(
    literal: &mut Literal,
    condition: bool,
    path: Option<&[(Interval, PathState)]>,
    data: &mut Data,
    msg_data: &mut MessageData,
    sender: &Option<mpsc::Sender<MSG>>,
) -> Result<Literal, ErrorInfo> {
    if let Some(path) = path {
        let path = resolve_path(path, condition, data, msg_data, sender)?;
        let (mut new_literal, ..) = exec_path_actions(
            literal,
            condition,
            None,
            &Some(path),
            &ContentType::get(&literal),
            msg_data,
            sender,
        )?;

        //TODO: remove this condition when 'msg_data' and 'sender' can be access anywhere in the code
        if new_literal.content_type == "string" {
            let string = serde_json::json!(new_literal.primitive.to_string());
            new_literal = interpolate(&string, new_literal.interval, data, msg_data, sender)?;
        }

        Ok(new_literal)
    } else {
        Ok(literal.to_owned())
    }
}

fn init_new_scope<'a>(data: &'a Data, context: &'a mut Context) -> Data<'a> {
    Data::new(
        &data.flows,
        &data.flow,
        context,
        &data.event,
        HashMap::new(),
        &data.custom_component,
        &data.native_component,
    )
}

fn insert_args_in_scope_memory(
    new_scope_data: &mut Data,
    fn_args: &Vec<String>,
    args: &ArgsType,
    msg_data: &mut MessageData,
    sender: &Option<mpsc::Sender<MSG>>,
) {
    for (index, name) in fn_args.iter().enumerate() {
        let value = args.get(name, index).unwrap();

        save_literal_in_mem(
            value.to_owned(),
            name.to_owned(),
            &MemoryType::Use,
            true,
            new_scope_data,
            msg_data,
            sender,
        );
    }
}

fn exec_fn_in_new_scope(expr: Expr, new_scope_data: &mut Data) -> Result<Literal, ErrorInfo> {
    match expr {
        Expr::Scope {
            block_type: BlockType::Function,
            scope,
            range: RangeInterval { start, .. },
        } => interpret_function_scope(&scope, new_scope_data, start),
        _ => panic!("error in parsing need to be expr scope"),
    }
}

fn normal_object_to_literal(
    name: &str,
    args: &Expr,
    interval: Interval,
    data: &mut Data,
    msg_data: &mut MessageData,
    sender: &Option<mpsc::Sender<MSG>>,
) -> Result<Literal, ErrorInfo> {
    let args = resolve_fn_args(args, data, msg_data, sender)?;

    let function =
        match data
            .flow
            .flow_instructions
            .get_key_value(&InstructionScope::FunctionScope {
                name: name.to_owned(),
                args: Vec::new(),
            }) {
            Some((i, e)) => Some((i.to_owned(), e.to_owned())),
            None => None,
        };
    let import = {
        match data
            .flow
            .flow_instructions
            .get_key_value(&InstructionScope::ImportScope(ImportScope {
                name: name.to_owned(),
                original_name: None,
                from_flow: None,
                position: Position::new(interval.clone()),
            })) {
            Some((InstructionScope::ImportScope(import), _expr)) => {
                match search_function(data.flows, import) {
                    Ok((fn_args, expr, new_flow)) => Some((fn_args, expr, new_flow)), // if new_flow == data.flow {
                    _err => None,
                }
            }
            _ => None,
        }
    };

    match (
        data.native_component.contains_key(name),
        BUILT_IN.contains(&name),
        function,
        import,
    ) {
        (true, ..) => {
            let value = match_native_builtin(&name, args, interval.to_owned(), data);
            Ok(MSG::send_error_msg(&sender, msg_data, value))
        }

        (_, true, ..) => {
            let value = match_builtin(&name, args, interval.to_owned(), data, msg_data, sender);

            Ok(MSG::send_error_msg(&sender, msg_data, value))
        }

        (
            ..,
            Some((
                InstructionScope::FunctionScope {
                    name: _,
                    args: fn_args,
                },
                expr,
            )),
            _,
        ) => {
            if fn_args.len() > args.len() {
                return Err(gen_error_info(
                    Position::new(interval),
                    ERROR_FN_ARGS.to_owned(),
                ));
            }

            let mut context = Context {
                current: HashMap::new(),
                metadata: HashMap::new(),
                api_info: data.context.api_info.clone(),
                hold: None,
                step: data.context.step.clone(),
                flow: data.context.flow.clone(),
            };

            let mut new_scope_data = init_new_scope(data, &mut context);

            insert_args_in_scope_memory(&mut new_scope_data, &fn_args, &args, msg_data, sender);

            exec_fn_in_new_scope(expr, &mut new_scope_data)
        }

        (.., Some((fn_args, expr, new_flow))) => {
            if fn_args.len() > args.len() {
                return Err(gen_error_info(
                    Position::new(interval),
                    ERROR_FN_ARGS.to_owned(),
                ));
            }

            let mut context = Context {
                current: HashMap::new(),
                metadata: HashMap::new(),
                api_info: data.context.api_info.clone(),
                hold: None,
                step: data.context.step.clone(),
                flow: data.context.flow.clone(),
            };

            let mut new_scope_data = init_new_scope(data, &mut context);
            new_scope_data.flow = new_flow;

            insert_args_in_scope_memory(&mut new_scope_data, &fn_args, &args, msg_data, sender);

            exec_fn_in_new_scope(expr, &mut new_scope_data)
        }

        _ => {
            let err = gen_error_info(
                Position::new(interval),
                format!("{} [{}]", ERROR_BUILTIN_UNKNOWN, name),
            );
            Ok(MSG::send_error_msg(
                &sender,
                msg_data,
                Err(err) as Result<Literal, ErrorInfo>,
            ))
        }
    }
}

pub fn expr_to_literal(
    expr: &Expr,
    condition: bool,
    path: Option<&[(Interval, PathState)]>,
    data: &mut Data,
    msg_data: &mut MessageData,
    sender: &Option<mpsc::Sender<MSG>>,
) -> Result<Literal, ErrorInfo> {
    match expr {
        Expr::ObjectExpr(ObjectType::As(name, var)) => {
            let value = expr_to_literal(var, condition, None, data, msg_data, sender)?;
            data.step_vars.insert(name.ident.to_owned(), value.clone());
            Ok(value)
        }
        Expr::PathExpr { literal, path } => {
            expr_to_literal(literal, condition, Some(path), data, msg_data, sender)
        }
        Expr::ObjectExpr(ObjectType::Normal(Function {
            name,
            args,
            interval,
        })) => {
            let mut literal =
                normal_object_to_literal(&name, args, *interval, data, msg_data, sender)?;

            exec_path_literal(&mut literal, condition, path, data, msg_data, sender)
        }
        Expr::MapExpr(map, RangeInterval { start, .. }) => {
            let mut object = HashMap::new();

            for (key, value) in map.iter() {
                object.insert(
                    key.to_owned(),
                    expr_to_literal(&value, condition, None, data, msg_data, sender)?,
                );
            }
            let mut literal = PrimitiveObject::get_literal(&object, start.to_owned());
            exec_path_literal(&mut literal, condition, path, data, msg_data, sender)
        }
        Expr::ComplexLiteral(vec, RangeInterval { start, .. }) => {
            let mut string =
                get_string_from_complex_string(vec, start.to_owned(), data, msg_data, sender)?;
            exec_path_literal(&mut string, condition, path, data, msg_data, sender)
        }
        Expr::VecExpr(vec, range) => {
            let mut array = vec![];
            for value in vec.iter() {
                array.push(expr_to_literal(
                    value, condition, None, data, msg_data, sender,
                )?)
            }
            let mut literal = PrimitiveArray::get_literal(&array, range.start.to_owned());
            exec_path_literal(&mut literal, condition, path, data, msg_data, sender)
        }
        Expr::InfixExpr(infix, exp_1, exp_2) => {
            evaluate_condition(infix, exp_1, exp_2, data, msg_data, sender)
        }
        Expr::LitExpr(literal) => exec_path_literal(
            &mut literal.clone(),
            condition,
            path,
            data,
            msg_data,
            sender,
        ),
        Expr::IdentExpr(var, ..) => Ok(get_var(
            var.to_owned(),
            condition,
            path,
            data,
            msg_data,
            sender,
        )?),
        e => Err(gen_error_info(
            Position::new(interval_from_expr(e)),
            ERROR_EXPR_TO_LITERAL.to_owned(),
        )),
    }
}

pub fn resolve_fn_args(
    expr: &Expr,
    data: &mut Data,
    msg_data: &mut MessageData,
    sender: &Option<mpsc::Sender<MSG>>,
) -> Result<ArgsType, ErrorInfo> {
    match expr {
        Expr::VecExpr(vec, ..) => {
            let mut map = HashMap::new();
            let mut first = 0;
            let mut named_args = false;

            for (index, value) in vec.iter().enumerate() {
                match value {
                    Expr::ObjectExpr(ObjectType::Assign(name, var)) => {
                        let name = match **name {
                            Expr::IdentExpr(ref var, ..) => var,
                            _ => {
                                return Err(gen_error_info(
                                    Position::new(interval_from_expr(name)),
                                    "key must be of type string".to_owned(),
                                ))
                            }
                        };
                        named_args = true;

                        let literal = expr_to_literal(var, false, None, data, msg_data, sender)?;
                        map.insert(name.ident.to_owned(), literal);
                    }
                    expr => {
                        first += 1;
                        if named_args && first > 1 {
                            return Err(gen_error_info(
                                Position::new(interval_from_expr(expr)),
                                ERROR_EXPR_TO_LITERAL.to_owned(), // TODO: error mix of named args and anonymous args
                            ));
                        }
                        let literal = expr_to_literal(expr, false, None, data, msg_data, sender)?;
                        map.insert(format!("arg{}", index), literal);
                    }
                }
            }

            match named_args {
                true => Ok(ArgsType::Named(map)),
                false => Ok(ArgsType::Normal(map)),
            }
        }
        e => Err(gen_error_info(
            Position::new(interval_from_expr(e)),
            ERROR_EXPR_TO_LITERAL.to_owned(), //TODO: internal error fn args bad format
        )),
    }
}
