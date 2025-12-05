#[cfg(all(not(feature = "slim"), not(feature = "full_regex")))]
compile_error!("Enable either the `full_regex` (default) or `slim` feature for regex support.");

#[cfg(feature = "slim")]
pub(crate) use regex_lite::Regex;

#[cfg(not(feature = "slim"))]
pub(crate) use regex::Regex;
