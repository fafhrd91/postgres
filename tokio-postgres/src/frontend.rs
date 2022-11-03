//! Frontend message serialization.
#![allow(missing_docs)]

use byteorder::{BigEndian, ByteOrder};
use std::convert::TryFrom;
use std::error::Error;
use std::io;
use std::marker;

use ntex::util::{Buf, BufMut, BytesMut, BytesVec};

use postgres_protocol::{write_nullable, write_nullable_vec, IsNull, Oid};

#[inline]
fn write_body<F, E>(buf: &mut BytesMut, f: F) -> Result<(), E>
where
    F: FnOnce(&mut BytesMut) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf)?;

    let size = i32::try_from(buf.len() - base).unwrap();
    BigEndian::write_i32(&mut buf[base..], size);
    Ok(())
}

#[inline]
fn write_body_vec<F, E>(buf: &mut BytesVec, f: F) -> Result<(), E>
where
    F: FnOnce(&mut BytesVec) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf)?;

    let size = i32::try_from(buf.len() - base).unwrap();
    BigEndian::write_i32(&mut buf[base..], size);
    Ok(())
}

pub enum BindError {
    Conversion(Box<dyn Error>),
    Serialization(io::Error),
}

impl From<Box<dyn Error>> for BindError {
    #[inline]
    fn from(e: Box<dyn Error>) -> BindError {
        BindError::Conversion(e)
    }
}

impl From<io::Error> for BindError {
    #[inline]
    fn from(e: io::Error) -> BindError {
        BindError::Serialization(e)
    }
}

#[inline]
pub fn bind<I, J, F, T, K>(
    portal: &str,
    statement: &str,
    formats: I,
    values: J,
    mut serializer: F,
    result_formats: K,
    buf: &mut BytesMut,
) -> Result<(), BindError>
where
    I: IntoIterator<Item = i16>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut) -> Result<IsNull, Box<dyn Error>>,
    K: IntoIterator<Item = i16>,
{
    buf.put_u8(b'B');

    write_body(buf, |buf| {
        write_cstr(portal.as_bytes(), buf)?;
        write_cstr(statement.as_bytes(), buf)?;
        write_counted(
            formats,
            |f, buf| {
                buf.put_i16(f);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;
        write_counted(
            values,
            |v, buf| write_nullable(|buf| serializer(v, buf), buf),
            buf,
        )?;
        write_counted(
            result_formats,
            |f, buf| {
                buf.put_i16(f);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;

        Ok(())
    })
}

#[inline]
pub fn bind_vec<I, J, F, T, K>(
    portal: &str,
    statement: &str,
    formats: I,
    values: J,
    mut serializer: F,
    result_formats: K,
    buf: &mut BytesVec,
) -> Result<(), BindError>
where
    I: IntoIterator<Item = i16>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesVec) -> Result<IsNull, Box<dyn Error>>,
    K: IntoIterator<Item = i16>,
{
    buf.put_u8(b'B');

    write_body_vec(buf, |buf| {
        write_cstr_vec(portal.as_bytes(), buf)?;
        write_cstr_vec(statement.as_bytes(), buf)?;
        write_counted_vec(
            formats,
            |f, buf| {
                buf.put_i16(f);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;
        write_counted_vec(
            values,
            |v, buf| write_nullable_vec(|buf| serializer(v, buf), buf),
            buf,
        )?;
        write_counted_vec(
            result_formats,
            |f, buf| {
                buf.put_i16(f);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;

        Ok(())
    })
}

#[inline]
fn write_counted<I, T, F, E>(items: I, mut serializer: F, buf: &mut BytesMut) -> Result<(), E>
where
    I: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 2]);
    let mut count = 0;
    for item in items {
        serializer(item, buf)?;
        count += 1;
    }
    let count = i16::try_from(count).unwrap();
    BigEndian::write_i16(&mut buf[base..], count);

    Ok(())
}

#[inline]
fn write_counted_vec<I, T, F, E>(items: I, mut serializer: F, buf: &mut BytesVec) -> Result<(), E>
where
    I: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesVec) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 2]);
    let mut count = 0;
    for item in items {
        serializer(item, buf)?;
        count += 1;
    }
    let count = i16::try_from(count).unwrap();
    BigEndian::write_i16(&mut buf[base..], count);

    Ok(())
}

#[inline]
pub fn cancel_request(process_id: i32, secret_key: i32, buf: &mut BytesMut) {
    write_body(buf, |buf| {
        buf.put_i32(80_877_102);
        buf.put_i32(process_id);
        buf.put_i32(secret_key);
        Ok::<_, io::Error>(())
    })
    .unwrap();
}

#[inline]
pub fn close(variant: u8, name: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'C');
    write_body(buf, |buf| {
        buf.put_u8(variant);
        write_cstr(name.as_bytes(), buf)
    })
}

pub struct CopyData<T> {
    buf: T,
    len: i32,
}

impl<T> CopyData<T>
where
    T: Buf,
{
    pub fn new(buf: T) -> io::Result<CopyData<T>> {
        let len = buf
            .remaining()
            .checked_add(4)
            .and_then(|l| i32::try_from(l).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "message length overflow")
            })?;

        Ok(CopyData { buf, len })
    }

    pub fn write(self, out: &mut BytesMut) {
        out.put_u8(b'd');
        out.put_i32(self.len);
        out.put(self.buf);
    }
}

#[inline]
pub fn copy_done(buf: &mut BytesMut) {
    buf.put_u8(b'c');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn copy_fail(message: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'f');
    write_body(buf, |buf| write_cstr(message.as_bytes(), buf))
}

#[inline]
pub fn describe(variant: u8, name: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'D');
    write_body(buf, |buf| {
        buf.put_u8(variant);
        write_cstr(name.as_bytes(), buf)
    })
}

#[inline]
pub fn execute(portal: &str, max_rows: i32, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'E');
    write_body(buf, |buf| {
        write_cstr(portal.as_bytes(), buf)?;
        buf.put_i32(max_rows);
        Ok(())
    })
}

#[inline]
pub fn execute_vec(portal: &str, max_rows: i32, buf: &mut BytesVec) -> io::Result<()> {
    buf.put_u8(b'E');
    write_body_vec(buf, |buf| {
        write_cstr_vec(portal.as_bytes(), buf)?;
        buf.put_i32(max_rows);
        Ok(())
    })
}

#[inline]
pub fn parse<I>(name: &str, query: &str, param_types: I, buf: &mut BytesMut) -> io::Result<()>
where
    I: IntoIterator<Item = Oid>,
{
    buf.put_u8(b'P');
    write_body(buf, |buf| {
        write_cstr(name.as_bytes(), buf)?;
        write_cstr(query.as_bytes(), buf)?;
        write_counted(
            param_types,
            |t, buf| {
                buf.put_u32(t);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;
        Ok(())
    })
}

#[inline]
pub fn password_message(password: &[u8], buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'p');
    write_body(buf, |buf| write_cstr(password, buf))
}

#[inline]
pub fn query(query: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'Q');
    write_body(buf, |buf| write_cstr(query.as_bytes(), buf))
}

#[inline]
pub fn sasl_initial_response(mechanism: &str, data: &[u8], buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'p');
    write_body(buf, |buf| {
        write_cstr(mechanism.as_bytes(), buf)?;
        let len = i32::try_from(data.len()).unwrap();
        buf.put_i32(len);
        buf.put_slice(data);
        Ok(())
    })
}

#[inline]
pub fn sasl_response(data: &[u8], buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'p');
    write_body(buf, |buf| {
        buf.put_slice(data);
        Ok(())
    })
}

#[inline]
pub fn ssl_request(buf: &mut BytesMut) {
    write_body(buf, |buf| {
        buf.put_i32(80_877_103);
        Ok::<_, io::Error>(())
    })
    .unwrap();
}

#[inline]
pub fn startup_message<'a, I>(parameters: I, buf: &mut BytesMut) -> io::Result<()>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    write_body(buf, |buf| {
        buf.put_i32(196_608);
        for (key, value) in parameters {
            write_cstr(key.as_bytes(), buf)?;
            write_cstr(value.as_bytes(), buf)?;
        }
        buf.put_u8(0);
        Ok(())
    })
}

#[inline]
pub fn sync(buf: &mut BytesMut) {
    buf.put_u8(b'S');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn sync_vec(buf: &mut BytesVec) {
    buf.put_u8(b'S');
    write_body_vec(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn terminate(buf: &mut BytesMut) {
    buf.put_u8(b'X');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
fn write_cstr(s: &[u8], buf: &mut BytesMut) -> Result<(), io::Error> {
    if s.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string contains embedded null",
        ));
    }
    buf.put_slice(s);
    buf.put_u8(0);
    Ok(())
}

#[inline]
fn write_cstr_vec(s: &[u8], buf: &mut BytesVec) -> Result<(), io::Error> {
    if s.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string contains embedded null",
        ));
    }
    buf.put_slice(s);
    buf.put_u8(0);
    Ok(())
}
