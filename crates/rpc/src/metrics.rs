use crate::Api;

pub trait FallibleResponse {
    fn error_kind(&self) -> Option<&'static str>;
}

impl<T, E: ErrorResponse> FallibleResponse for Result<T, E> {
    fn error_kind(&self) -> Option<&'static str> {
        self.as_ref().err().map(ErrorResponse::kind)
    }
}

pub trait ErrorResponse {
    fn kind(&self) -> &'static str;
}

pub(crate) fn rpc_id_label<API: Api>(id: u8) -> &'static str {
    API::RpcId::try_from(id)
        .map(Into::into)
        .unwrap_or("unknown")
}
