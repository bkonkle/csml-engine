use crate::error_format::{gen_error_info, ErrorInfo, ERROR_EXTENSION_NAME};
use crate::{
    data::{ast::*, position::Position, ArgsType, Data, Literal, MessageData, MSG},
    error_format::ERROR_EXTENSION_UNKNOWN,
};
use std::sync::mpsc;

pub fn extension(
    args: ArgsType,
    interval: Interval,
    data: &mut Data,
    msg_data: &mut MessageData,
    sender: &Option<mpsc::Sender<MSG>>,
) -> Result<Literal, ErrorInfo> {
    let extensions = data
        .context
        .extension_info
        .clone()
        .map(|x| x.function_map)
        .unwrap_or_default();

    if let Some(name) = args.get("name", 0).map(|a| a.primitive.to_string()) {
        let ext = extensions.get(&name).cloned();

        if let Some(ext) = ext {
            ext.execute(args, interval, data, msg_data, sender)
        } else {
            Err(gen_error_info(
                Position::new(interval, &data.context.flow),
                format!("{} [{}]", ERROR_EXTENSION_UNKNOWN, name),
            ))
        }
    } else {
        Err(gen_error_info(
            Position::new(interval, &data.context.flow),
            ERROR_EXTENSION_NAME.to_owned(),
        ))
    }
}
