1. errc(status, code, format)
`errc()` is not present in libc. `errc` signature: errc(status, code, format). It used for posix threads, who don't set up errno when they fail, to covert the err code into readable content. The difference between `errc()` and `err()` is simply that `errc()` takes another arg `code`.
One can set errno manually and use `err()` instead.