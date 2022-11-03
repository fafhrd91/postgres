//! Low level Postgres protocol APIs.
//!
//! This crate implements the low level components of Postgres's communication
//! protocol, including message and value serialization and deserialization.
//! It is designed to be used as a building block by higher level APIs such as
//! `rust-postgres`, and should not typically be used directly.
//!
//! # Note
//!
//! This library assumes that the `client_encoding` backend parameter has been
//! set to `UTF8`. It will most likely not behave properly if that is not the case.
#![doc(html_root_url = "https://docs.rs/postgres-protocol/0.5")]
// #![warn(missing_docs, rust_2018_idioms, clippy::all)]

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut, BytesVec};
use std::io;

pub mod authentication;
pub mod message;
pub mod types;

/// A Postgres OID.
pub type Oid = u32;

/// An enum indicating if a value is `NULL` or not.
pub enum IsNull {
    /// The value is `NULL`.
    Yes,
    /// The value is not `NULL`.
    No,
}

pub fn write_nullable<F, E>(serializer: F, buf: &mut BytesMut) -> Result<(), E>
where
    F: FnOnce(&mut BytesMut) -> Result<IsNull, E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.put_i32(0);
    let size = match serializer(buf)? {
        IsNull::No => i32::from_usize(buf.len() - base - 4)?,
        IsNull::Yes => -1,
    };
    BigEndian::write_i32(&mut buf[base..], size);

    Ok(())
}

pub fn write_nullable_vec<F, E>(serializer: F, buf: &mut BytesVec) -> Result<(), E>
where
    F: FnOnce(&mut BytesVec) -> Result<IsNull, E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.put_i32(0);
    let size = match serializer(buf)? {
        IsNull::No => i32::from_usize(buf.len() - base - 4)?,
        IsNull::Yes => -1,
    };
    BigEndian::write_i32(&mut buf[base..], size);

    Ok(())
}

trait FromUsize: Sized {
    fn from_usize(x: usize) -> Result<Self, io::Error>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            #[inline]
            fn from_usize(x: usize) -> io::Result<$t> {
                if x > <$t>::max_value() as usize {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large to transmit",
                    ))
                } else {
                    Ok(x as $t)
                }
            }
        }
    };
}

from_usize!(i16);
from_usize!(i32);
