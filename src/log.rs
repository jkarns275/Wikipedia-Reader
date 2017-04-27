/// Contains macros for logging. The macros are essentially wrappers around
/// the println macro.
#[macro_export]
macro_rules! log {
    ($r:expr, $s:expr) => ({
        print!("\x1b[32;1m[{}] \x1b[0m", $r);
        println!($s)
    });
    ($r:expr, $fmt:expr, $($arg:tt)*) => ({
        print!("\x1b[32;1m[{}] \x1b[0m", $r);
        print!(concat!($fmt, "\n"), $($arg)*);
    });
}

#[macro_export]
macro_rules! error {
    ($s:expr) => ({
        print!("\x1b[31;1m[Error] \x1b[0m");
        println!($s);
    });
    ($fmt:expr, $($arg:tt)*) => ({
        print!("\x1b[31;1m[Error] \x1b[0m");
        print!(concat!($fmt, "\n"), $($arg)*);
    })
}
