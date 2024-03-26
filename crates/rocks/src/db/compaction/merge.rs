use {
    crate::{
        db::cf::Column,
        util::serde::{deserialize, serialize},
        Error,
    },
    rocksdb::MergeOperands,
    serde::{de::DeserializeOwned, Serialize},
    std::{
        marker::PhantomData,
        time::{Duration, Instant},
    },
    tap::TapFallible,
};

pub trait MergeOperator {
    type Output: Serialize + DeserializeOwned;
    type Input: DeserializeOwned;

    fn initialize(name: &str, opts: &mut rocksdb::Options);

    /// In this step we're merging multiple `Self::Input` operands together. The
    /// result should be a serialized final `Self::Input`.
    fn partial_merge(
        key: &[u8],
        output: Option<&[u8]>,
        input: &MergeOperands,
    ) -> Result<Vec<u8>, Error>;

    /// In this step we're applying `Self::Input` operands to `Self::Output`,
    /// and the result should be a serialized `Self::Output`.
    fn full_merge(
        key: &[u8],
        output: Option<&[u8]>,
        input: &MergeOperands,
    ) -> Result<Vec<u8>, Error>;
}

pub struct NoMerge;

impl MergeOperator for NoMerge {
    type Output = ();
    type Input = ();

    fn initialize(_name: &str, _opts: &mut rocksdb::Options) {
        // This is a no-op.
    }

    fn partial_merge(
        _key: &[u8],
        _output: Option<&[u8]>,
        _input: &MergeOperands,
    ) -> Result<Vec<u8>, Error> {
        unreachable!()
    }

    fn full_merge(
        _key: &[u8],
        _output: Option<&[u8]>,
        _input: &MergeOperands,
    ) -> Result<Vec<u8>, Error> {
        unreachable!()
    }
}

pub struct OverwriteMerge<T>(PhantomData<T>);

impl<T> MergeOperator for OverwriteMerge<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    type Output = T;
    type Input = T;

    fn initialize(name: &str, opts: &mut rocksdb::Options) {
        // This merge operator is associative by rocksdb definition.
        opts.set_merge_operator_associative(name, full_merge::<Self>);
    }

    fn partial_merge(
        key: &[u8],
        output: Option<&[u8]>,
        input: &MergeOperands,
    ) -> Result<Vec<u8>, Error> {
        Self::full_merge(key, output, input)
    }

    fn full_merge(
        _: &[u8],
        output: Option<&[u8]>,
        input: &MergeOperands,
    ) -> Result<Vec<u8>, Error> {
        // Save some cylcles by returning the last value directly, without
        // (de)serializing.
        input
            .into_iter()
            .last()
            .or(output)
            .map(Into::into)
            .ok_or(Error::Deserialize)
    }
}

pub trait Merge<I> {
    fn merge(&mut self, input: I);
}

pub struct AsymmetricMerge<C, I, O>(PhantomData<(C, I, O)>);

impl<C, I, O> MergeOperator for AsymmetricMerge<C, I, O>
where
    C: Column,
    I: Serialize + DeserializeOwned + Merge<I> + 'static,
    O: Default + Serialize + DeserializeOwned + Merge<I> + 'static,
{
    type Output = O;
    type Input = I;

    fn initialize(name: &str, opts: &mut rocksdb::Options) {
        opts.set_merge_operator(name, full_merge::<Self>, partial_merge::<Self>);
    }

    fn partial_merge(_: &[u8], _: Option<&[u8]>, input: &MergeOperands) -> Result<Vec<u8>, Error> {
        let time = Instant::now();
        let result = {
            let mut input = op_iter::<<Self as MergeOperator>::Input>(input);

            // Use the first merge operand as the output, and merge subsequent operands into
            // it.
            let mut output = input.next().ok_or(Error::Deserialize).tap_err(|_| {
                tracing::warn!("merge: insufficient number of merge operands");
            })?;

            for input in input {
                output.merge(input);
            }

            serialize(&output)
        };

        merge_metrics::<C>(time.elapsed(), "partial");

        result
    }

    fn full_merge(
        _: &[u8],
        output: Option<&[u8]>,
        input: &MergeOperands,
    ) -> Result<Vec<u8>, Error> {
        let time = Instant::now();
        let result = {
            let cf_name = C::NAME;
            let operator_name = std::any::type_name::<Self>();

            let mut output = output
                .map(deserialize::<Self::Output>)
                .transpose()
                .tap_err(|err| {
                    tracing::warn!(?err, %cf_name, %operator_name, "merge: failed to deserialize existing value");
                })?
                .unwrap_or_default();

            for input in op_iter(input) {
                output.merge(input);
            }

            serialize(&output)
        };

        merge_metrics::<C>(time.elapsed(), "full");

        result
    }
}

// TODO: Metrics counters incur some CPU cost due to mutex usage under the hood,
// so we should remove these once we've collected the data.
fn merge_metrics<C: Column>(elapsed: Duration, kind: &'static str) {
    use wc::metrics::otel::KeyValue;

    let elapsed = elapsed.as_micros() as f64;
    let kind_kv = KeyValue::new("kind", kind);
    let cf_name_kv = KeyValue::new("cf_name", C::NAME.as_str());

    wc::metrics::histogram!("rocksdb_merge_time", elapsed, &[kind_kv, cf_name_kv]);
}

fn op_iter<'a, T>(input: &'a MergeOperands) -> impl Iterator<Item = T> + 'a
where
    T: DeserializeOwned + 'a,
{
    input.into_iter().map(deserialize::<T>).filter_map(|op| {
        op.tap_err(|err| {
            tracing::warn!(?err, "merge: failed to deserialize merge operand");
        })
        .ok()
    })
}

fn partial_merge<M: MergeOperator>(
    key: &[u8],
    existing_value: Option<&[u8]>,
    ops: &MergeOperands,
) -> Option<Vec<u8>> {
    M::partial_merge(key, existing_value, ops)
        .tap_err(|err| {
            let operator_name = std::any::type_name::<M>();

            tracing::warn!(?err, %operator_name, "merge: failed to apply partial merge operation");
        })
        .ok()
}

fn full_merge<M: MergeOperator>(
    key: &[u8],
    existing_value: Option<&[u8]>,
    ops: &MergeOperands,
) -> Option<Vec<u8>> {
    M::full_merge(key, existing_value, ops)
        .tap_err(|err| {
            let operator_name = std::any::type_name::<M>();

            tracing::warn!(?err, %operator_name, "merge: failed to apply full merge operation");
        })
        .ok()
}
