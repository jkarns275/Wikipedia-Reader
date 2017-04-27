/// An incredibly useful macro. It will check an expression of type Error<T, Z>,
/// if it is Err(err) it will RETURN in whatever function it is placed in with
/// error, otherwise it will continue in the function. This cuts down on the amount
/// of error checking code that will clog things up.
/// Optionally, it will also store the value x in $v (e.g. if it is Ok(x), $v = x).
#[macro_export]
macro_rules! check {
    ( $e:expr ) => (
    match $e {
        Ok(_) => {},
    Err(e) => return Err(e)
        }
    );
    ( $e:expr, $v:ident) => (
        match $e {
            Ok(r) => $v = r,
            Err(e) => return Err(e)
        }
    )
}
