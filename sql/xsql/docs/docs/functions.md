### !

! expr - Logical not.

<br/>

### %

expr1 % expr2 - Returns the remainder after `expr1`/`expr2`.

**Examples:**

```
> SELECT 2 % 1.8;
 0.2
> SELECT MOD(2, 1.8);
 0.2
```

<br/>

### &

expr1 & expr2 - Returns the result of bitwise AND of `expr1` and `expr2`.

**Examples:**

```
> SELECT 3 & 5;
 1
```

<br/>

### *

expr1 * expr2 - Returns `expr1`*`expr2`.

**Examples:**

```
> SELECT 2 * 3;
 6
```

<br/>

### +

expr1 + expr2 - Returns `expr1`+`expr2`.

**Examples:**

```
> SELECT 1 + 2;
 3
```

<br/>

### -

expr1 - expr2 - Returns `expr1`-`expr2`.

**Examples:**

```
> SELECT 2 - 1;
 1
```

<br/>

### /

expr1 / expr2 - Returns `expr1`/`expr2`. It always performs floating point division.

**Examples:**

```
> SELECT 3 / 2;
 1.5
> SELECT 2L / 2L;
 1.0
```

<br/>

### <

expr1 < expr2 - Returns true if `expr1` is less than `expr2`.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be ordered. For example, map type is not orderable, so it
    is not supported. For complex types such array/struct, the data types of fields must
    be orderable.

**Examples:**

```
> SELECT 1 < 2;
 true
> SELECT 1.1 < '1';
 false
> SELECT to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52');
 false
> SELECT to_date('2009-07-30 04:17:52') < to_date('2009-08-01 04:17:52');
 true
> SELECT 1 < NULL;
 NULL
```

<br/>

### <=

expr1 <= expr2 - Returns true if `expr1` is less than or equal to `expr2`.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be ordered. For example, map type is not orderable, so it
    is not supported. For complex types such array/struct, the data types of fields must
    be orderable.

**Examples:**

```
> SELECT 2 <= 2;
 true
> SELECT 1.0 <= '1';
 true
> SELECT to_date('2009-07-30 04:17:52') <= to_date('2009-07-30 04:17:52');
 true
> SELECT to_date('2009-07-30 04:17:52') <= to_date('2009-08-01 04:17:52');
 true
> SELECT 1 <= NULL;
 NULL
```

<br/>

### <=>

expr1 <=> expr2 - Returns same result as the EQUAL(=) operator for non-null operands,
but returns true if both are null, false if one of the them is null.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be used in equality comparison. Map type is not supported.
    For complex types such array/struct, the data types of fields must be orderable.

**Examples:**

```
> SELECT 2 <=> 2;
 true
> SELECT 1 <=> '1';
 true
> SELECT true <=> NULL;
 false
> SELECT NULL <=> NULL;
 true
```

<br/>

### =

expr1 = expr2 - Returns true if `expr1` equals `expr2`, or false otherwise.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be used in equality comparison. Map type is not supported.
    For complex types such array/struct, the data types of fields must be orderable.

**Examples:**

```
> SELECT 2 = 2;
 true
> SELECT 1 = '1';
 true
> SELECT true = NULL;
 NULL
> SELECT NULL = NULL;
 NULL
```

<br/>

### ==

expr1 == expr2 - Returns true if `expr1` equals `expr2`, or false otherwise.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be used in equality comparison. Map type is not supported.
    For complex types such array/struct, the data types of fields must be orderable.

**Examples:**

```
> SELECT 2 == 2;
 true
> SELECT 1 == '1';
 true
> SELECT true == NULL;
 NULL
> SELECT NULL == NULL;
 NULL
```

<br/>

### >

expr1 > expr2 - Returns true if `expr1` is greater than `expr2`.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be ordered. For example, map type is not orderable, so it
    is not supported. For complex types such array/struct, the data types of fields must
    be orderable.

**Examples:**

```
> SELECT 2 > 1;
 true
> SELECT 2 > '1.1';
 true
> SELECT to_date('2009-07-30 04:17:52') > to_date('2009-07-30 04:17:52');
 false
> SELECT to_date('2009-07-30 04:17:52') > to_date('2009-08-01 04:17:52');
 false
> SELECT 1 > NULL;
 NULL
```

<br/>

### >=

expr1 >= expr2 - Returns true if `expr1` is greater than or equal to `expr2`.

**Arguments:**

* expr1, expr2 - the two expressions must be same type or can be casted to a common type,
    and must be a type that can be ordered. For example, map type is not orderable, so it
    is not supported. For complex types such array/struct, the data types of fields must
    be orderable.

**Examples:**

```
> SELECT 2 >= 1;
 true
> SELECT 2.0 >= '2.1';
 false
> SELECT to_date('2009-07-30 04:17:52') >= to_date('2009-07-30 04:17:52');
 true
> SELECT to_date('2009-07-30 04:17:52') >= to_date('2009-08-01 04:17:52');
 false
> SELECT 1 >= NULL;
 NULL
```

<br/>

### ^

expr1 ^ expr2 - Returns the result of bitwise exclusive OR of `expr1` and `expr2`.

**Examples:**

```
> SELECT 3 ^ 5;
 2
```

<br/>

### abs

abs(expr) - Returns the absolute value of the numeric value.

**Examples:**

```
> SELECT abs(-1);
 1
```

<br/>

### acos

acos(expr) - Returns the inverse cosine (a.k.a. arc cosine) of `expr`, as if computed by
`java.lang.Math.acos`.

**Examples:**

```
> SELECT acos(1);
 0.0
> SELECT acos(2);
 NaN
```

<br/>

### add_months

add_months(start_date, num_months) - Returns the date that is `num_months` after `start_date`.

**Examples:**

```
> SELECT add_months('2016-08-31', 1);
 2016-09-30
```

**Since:** 1.5.0

<br/>

### and

expr1 and expr2 - Logical AND.

<br/>

### approx_count_distinct

approx_count_distinct(expr[, relativeSD]) - Returns the estimated cardinality by HyperLogLog++.
`relativeSD` defines the maximum estimation error allowed.

<br/>

### approx_percentile

approx_percentile(col, percentage [, accuracy]) - Returns the approximate percentile value of numeric
column `col` at the given percentage. The value of percentage must be between 0.0
and 1.0. The `accuracy` parameter (default: 10000) is a positive numeric literal which
controls approximation accuracy at the cost of memory. Higher value of `accuracy` yields
better accuracy, `1.0/accuracy` is the relative error of the approximation.
When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
In this case, returns the approximate percentile array of column `col` at the given
percentage array.

**Examples:**

```
> SELECT approx_percentile(10.0, array(0.5, 0.4, 0.1), 100);
 [10.0,10.0,10.0]
> SELECT approx_percentile(10.0, 0.5, 100);
 10.0
```

<br/>

### array

array(expr, ...) - Returns an array with the given elements.

**Examples:**

```
> SELECT array(1, 2, 3);
 [1,2,3]
```

<br/>

### array_contains

array_contains(array, value) - Returns true if the array contains the value.

**Examples:**

```
> SELECT array_contains(array(1, 2, 3), 2);
 true
```

<br/>

### ascii

ascii(str) - Returns the numeric value of the first character of `str`.

**Examples:**

```
> SELECT ascii('222');
 50
> SELECT ascii(2);
 50
```

<br/>

### asin

asin(expr) - Returns the inverse sine (a.k.a. arc sine) the arc sin of `expr`,
as if computed by `java.lang.Math.asin`.

**Examples:**

```
> SELECT asin(0);
 0.0
> SELECT asin(2);
 NaN
```

<br/>

### assert_true

assert_true(expr) - Throws an exception if `expr` is not true.

**Examples:**

```
> SELECT assert_true(0 < 1);
 NULL
```

<br/>

### atan

atan(expr) - Returns the inverse tangent (a.k.a. arc tangent) of `expr`, as if computed by
`java.lang.Math.atan`

**Examples:**

```
> SELECT atan(0);
 0.0
```

<br/>

### atan2

atan2(exprY, exprX) - Returns the angle in radians between the positive x-axis of a plane
and the point given by the coordinates (`exprX`, `exprY`), as if computed by
`java.lang.Math.atan2`.

**Arguments:**

* exprY - coordinate on y-axis
* exprX - coordinate on x-axis

**Examples:**

```
> SELECT atan2(0, 0);
 0.0
```

<br/>

### avg

avg(expr) - Returns the mean calculated from values of a group.

<br/>

### base64

base64(bin) - Converts the argument from a binary `bin` to a base 64 string.

**Examples:**

```
> SELECT base64('Spark SQL');
 U3BhcmsgU1FM
```

<br/>

### bigint

bigint(expr) - Casts the value `expr` to the target data type `bigint`.

<br/>

### bin

bin(expr) - Returns the string representation of the long value `expr` represented in binary.

**Examples:**

```
> SELECT bin(13);
 1101
> SELECT bin(-13);
 1111111111111111111111111111111111111111111111111111111111110011
> SELECT bin(13.3);
 1101
```

<br/>

### binary

binary(expr) - Casts the value `expr` to the target data type `binary`.

<br/>

### bit_length

bit_length(expr) - Returns the bit length of string data or number of bits of binary data.

**Examples:**

```
> SELECT bit_length('Spark SQL');
 72
```

<br/>

### boolean

boolean(expr) - Casts the value `expr` to the target data type `boolean`.

<br/>

### bround

bround(expr, d) - Returns `expr` rounded to `d` decimal places using HALF_EVEN rounding mode.

**Examples:**

```
> SELECT bround(2.5, 0);
 2.0
```

<br/>

### cast

cast(expr AS type) - Casts the value `expr` to the target data type `type`.

**Examples:**

```
> SELECT cast('10' as int);
 10
```

<br/>

### cbrt

cbrt(expr) - Returns the cube root of `expr`.

**Examples:**

```
> SELECT cbrt(27.0);
 3.0
```

<br/>

### ceil

ceil(expr) - Returns the smallest integer not smaller than `expr`.

**Examples:**

```
> SELECT ceil(-0.1);
 0
> SELECT ceil(5);
 5
```

<br/>

### ceiling

ceiling(expr) - Returns the smallest integer not smaller than `expr`.

**Examples:**

```
> SELECT ceiling(-0.1);
 0
> SELECT ceiling(5);
 5
```

<br/>

### char

char(expr) - Returns the ASCII character having the binary equivalent to `expr`. If n is larger than 256 the result is equivalent to chr(n % 256)

**Examples:**

```
> SELECT char(65);
 A
```

<br/>

### char_length

char_length(expr) - Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.

**Examples:**

```
> SELECT char_length('Spark SQL ');
 10
> SELECT CHAR_LENGTH('Spark SQL ');
 10
> SELECT CHARACTER_LENGTH('Spark SQL ');
 10
```

<br/>

### character_length

character_length(expr) - Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.

**Examples:**

```
> SELECT character_length('Spark SQL ');
 10
> SELECT CHAR_LENGTH('Spark SQL ');
 10
> SELECT CHARACTER_LENGTH('Spark SQL ');
 10
```

<br/>

### chr

chr(expr) - Returns the ASCII character having the binary equivalent to `expr`. If n is larger than 256 the result is equivalent to chr(n % 256)

**Examples:**

```
> SELECT chr(65);
 A
```

<br/>

### coalesce

coalesce(expr1, expr2, ...) - Returns the first non-null argument if exists. Otherwise, null.

**Examples:**

```
> SELECT coalesce(NULL, 1, NULL);
 1
```

<br/>

### collect_list

collect_list(expr) - Collects and returns a list of non-unique elements.

<br/>

### collect_set

collect_set(expr) - Collects and returns a set of unique elements.

<br/>

### concat

concat(str1, str2, ..., strN) - Returns the concatenation of str1, str2, ..., strN.

**Examples:**

```
> SELECT concat('Spark', 'SQL');
 SparkSQL
```

<br/>

### concat_ws

concat_ws(sep, [str | array(str)]+) - Returns the concatenation of the strings separated by `sep`.

**Examples:**

```
> SELECT concat_ws(' ', 'Spark', 'SQL');
  Spark SQL
```

<br/>

### conv

conv(num, from_base, to_base) - Convert `num` from `from_base` to `to_base`.

**Examples:**

```
> SELECT conv('100', 2, 10);
 4
> SELECT conv(-10, 16, -10);
 -16
```

<br/>

### corr

corr(expr1, expr2) - Returns Pearson coefficient of correlation between a set of number pairs.

<br/>

### cos

cos(expr) - Returns the cosine of `expr`, as if computed by
`java.lang.Math.cos`.

**Arguments:**

* expr - angle in radians

**Examples:**

```
> SELECT cos(0);
 1.0
```

<br/>

### cosh

cosh(expr) - Returns the hyperbolic cosine of `expr`, as if computed by
`java.lang.Math.cosh`.

**Arguments:**

* expr - hyperbolic angle

**Examples:**

```
> SELECT cosh(0);
 1.0
```

<br/>

### cot

cot(expr) - Returns the cotangent of `expr`, as if computed by `1/java.lang.Math.cot`.

**Arguments:**

* expr - angle in radians

**Examples:**

```
> SELECT cot(1);
 0.6420926159343306
```

<br/>

### count

count(*) - Returns the total number of retrieved rows, including rows containing null.

count(expr) - Returns the number of rows for which the supplied expression is non-null.

count(DISTINCT expr[, expr...]) - Returns the number of rows for which the supplied expression(s) are unique and non-null.

<br/>

### count_min_sketch

count_min_sketch(col, eps, confidence, seed) - Returns a count-min sketch of a column with the given esp,
confidence and seed. The result is an array of bytes, which can be deserialized to a
`CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for
cardinality estimation using sub-linear space.

<br/>

### covar_pop

covar_pop(expr1, expr2) - Returns the population covariance of a set of number pairs.

<br/>

### covar_samp

covar_samp(expr1, expr2) - Returns the sample covariance of a set of number pairs.

<br/>

### crc32

crc32(expr) - Returns a cyclic redundancy check value of the `expr` as a bigint.

**Examples:**

```
> SELECT crc32('Spark');
 1557323817
```

<br/>

### cube

<br/>

### cume_dist

cume_dist() - Computes the position of a value relative to all values in the partition.

<br/>

### current_database

current_database() - Returns the current database.

**Examples:**

```
> SELECT current_database();
 default
```

<br/>

### current_date

current_date() - Returns the current date at the start of query evaluation.

**Since:** 1.5.0

<br/>

### current_timestamp

current_timestamp() - Returns the current timestamp at the start of query evaluation.

**Since:** 1.5.0

<br/>

### date

date(expr) - Casts the value `expr` to the target data type `date`.

<br/>

### date_add

date_add(start_date, num_days) - Returns the date that is `num_days` after `start_date`.

**Examples:**

```
> SELECT date_add('2016-07-30', 1);
 2016-07-31
```

**Since:** 1.5.0

<br/>

### date_format

date_format(timestamp, fmt) - Converts `timestamp` to a value of string in the format specified by the date format `fmt`.

**Examples:**

```
> SELECT date_format('2016-04-08', 'y');
 2016
```

**Since:** 1.5.0

<br/>

### date_sub

date_sub(start_date, num_days) - Returns the date that is `num_days` before `start_date`.

**Examples:**

```
> SELECT date_sub('2016-07-30', 1);
 2016-07-29
```

**Since:** 1.5.0

<br/>

### date_trunc

date_trunc(fmt, ts) - Returns timestamp `ts` truncated to the unit specified by the format model `fmt`.
`fmt` should be one of ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD", "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"]

**Examples:**

```
> SELECT date_trunc('2015-03-05T09:32:05.359', 'YEAR');
 2015-01-01T00:00:00
> SELECT date_trunc('2015-03-05T09:32:05.359', 'MM');
 2015-03-01T00:00:00
> SELECT date_trunc('2015-03-05T09:32:05.359', 'DD');
 2015-03-05T00:00:00
> SELECT date_trunc('2015-03-05T09:32:05.359', 'HOUR');
 2015-03-05T09:00:00
```

**Since:** 2.3.0

<br/>

### datediff

datediff(endDate, startDate) - Returns the number of days from `startDate` to `endDate`.

**Examples:**

```
> SELECT datediff('2009-07-31', '2009-07-30');
 1

> SELECT datediff('2009-07-30', '2009-07-31');
 -1
```

**Since:** 1.5.0

<br/>

### day

day(date) - Returns the day of month of the date/timestamp.

**Examples:**

```
> SELECT day('2009-07-30');
 30
```

**Since:** 1.5.0

<br/>

### dayofmonth

dayofmonth(date) - Returns the day of month of the date/timestamp.

**Examples:**

```
> SELECT dayofmonth('2009-07-30');
 30
```

**Since:** 1.5.0

<br/>

### dayofweek

dayofweek(date) - Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

**Examples:**

```
> SELECT dayofweek('2009-07-30');
 5
```

**Since:** 2.3.0

<br/>

### dayofyear

dayofyear(date) - Returns the day of year of the date/timestamp.

**Examples:**

```
> SELECT dayofyear('2016-04-09');
 100
```

**Since:** 1.5.0

<br/>

### decimal

decimal(expr) - Casts the value `expr` to the target data type `decimal`.

<br/>

### decode

decode(bin, charset) - Decodes the first argument using the second argument character set.

**Examples:**

```
> SELECT decode(encode('abc', 'utf-8'), 'utf-8');
 abc
```

<br/>

### degrees

degrees(expr) - Converts radians to degrees.

**Arguments:**

* expr - angle in radians

**Examples:**

```
> SELECT degrees(3.141592653589793);
 180.0
```

<br/>

### dense_rank

dense_rank() - Computes the rank of a value in a group of values. The result is one plus the
previously assigned rank value. Unlike the function rank, dense_rank will not produce gaps
in the ranking sequence.

<br/>

### double

double(expr) - Casts the value `expr` to the target data type `double`.

<br/>

### e

e() - Returns Euler's number, e.

**Examples:**

```
> SELECT e();
 2.718281828459045
```

<br/>

### elt

elt(n, input1, input2, ...) - Returns the `n`-th input, e.g., returns `input2` when `n` is 2.

**Examples:**

```
> SELECT elt(1, 'scala', 'java');
 scala
```

<br/>

### encode

encode(str, charset) - Encodes the first argument using the second argument character set.

**Examples:**

```
> SELECT encode('abc', 'utf-8');
 abc
```

<br/>

### exp

exp(expr) - Returns e to the power of `expr`.

**Examples:**

```
> SELECT exp(0);
 1.0
```

<br/>

### explode

explode(expr) - Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns.

**Examples:**

```
> SELECT explode(array(10, 20));
 10
 20
```

<br/>

### explode_outer

explode_outer(expr) - Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns.

**Examples:**

```
> SELECT explode_outer(array(10, 20));
 10
 20
```

<br/>

### expm1

expm1(expr) - Returns exp(`expr`) - 1.

**Examples:**

```
> SELECT expm1(0);
 0.0
```

<br/>

### factorial

factorial(expr) - Returns the factorial of `expr`. `expr` is [0..20]. Otherwise, null.

**Examples:**

```
> SELECT factorial(5);
 120
```

<br/>

### find_in_set

find_in_set(str, str_array) - Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`str_array`).
Returns 0, if the string was not found or if the given string (`str`) contains a comma.

**Examples:**

```
> SELECT find_in_set('ab','abc,b,ab,c,def');
 3
```

<br/>

### first

first(expr[, isIgnoreNull]) - Returns the first value of `expr` for a group of rows.
If `isIgnoreNull` is true, returns only non-null values.

<br/>

### first_value

first_value(expr[, isIgnoreNull]) - Returns the first value of `expr` for a group of rows.
If `isIgnoreNull` is true, returns only non-null values.

<br/>

### float

float(expr) - Casts the value `expr` to the target data type `float`.

<br/>

### floor

floor(expr) - Returns the largest integer not greater than `expr`.

**Examples:**

```
> SELECT floor(-0.1);
 -1
> SELECT floor(5);
 5
```

<br/>

### format_number

format_number(expr1, expr2) - Formats the number `expr1` like '#,###,###.##', rounded to `expr2`
decimal places. If `expr2` is 0, the result has no decimal point or fractional part.
This is supposed to function like MySQL's FORMAT.

**Examples:**

```
> SELECT format_number(12332.123456, 4);
 12,332.1235
```

<br/>

### format_string

format_string(strfmt, obj, ...) - Returns a formatted string from printf-style format strings.

**Examples:**

```
> SELECT format_string("Hello World %d %s", 100, "days");
 Hello World 100 days
```

<br/>

### from_json

from_json(jsonStr, schema[, options]) - Returns a struct value with the given `jsonStr` and `schema`.

**Examples:**

```
> SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE');
 {"a":1, "b":0.8}
> SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
 {"time":"2015-08-26 00:00:00.0"}
```

**Since:** 2.2.0

<br/>

### from_unixtime

from_unixtime(unix_time, format) - Returns `unix_time` in the specified `format`.

**Examples:**

```
> SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss');
 1970-01-01 00:00:00
```

**Since:** 1.5.0

<br/>

### from_utc_timestamp

from_utc_timestamp(timestamp, timezone) - Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders that time as a timestamp in the given time zone. For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'.

**Examples:**

```
> SELECT from_utc_timestamp('2016-08-31', 'Asia/Seoul');
 2016-08-31 09:00:00
```

**Since:** 1.5.0

<br/>

### get_json_object

get_json_object(json_txt, path) - Extracts a json object from `path`.

**Examples:**

```
> SELECT get_json_object('{"a":"b"}', '$.a');
 b
```

<br/>

### greatest

greatest(expr, ...) - Returns the greatest value of all parameters, skipping null values.

**Examples:**

```
> SELECT greatest(10, 9, 2, 4, 3);
 10
```

<br/>

### grouping

<br/>

### grouping_id

<br/>

### hash

hash(expr1, expr2, ...) - Returns a hash value of the arguments.

**Examples:**

```
> SELECT hash('Spark', array(123), 2);
 -1321691492
```

<br/>

### hex

hex(expr) - Converts `expr` to hexadecimal.

**Examples:**

```
> SELECT hex(17);
 11
> SELECT hex('Spark SQL');
 537061726B2053514C
```

<br/>

### hour

hour(timestamp) - Returns the hour component of the string/timestamp.

**Examples:**

```
> SELECT hour('2009-07-30 12:58:59');
 12
```

**Since:** 1.5.0

<br/>

### hypot

hypot(expr1, expr2) - Returns sqrt(`expr1`**2 + `expr2`**2).

**Examples:**

```
> SELECT hypot(3, 4);
 5.0
```

<br/>

### if

if(expr1, expr2, expr3) - If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.

**Examples:**

```
> SELECT if(1 < 2, 'a', 'b');
 a
```

<br/>

### ifnull

ifnull(expr1, expr2) - Returns `expr2` if `expr1` is null, or `expr1` otherwise.

**Examples:**

```
> SELECT ifnull(NULL, array('2'));
 ["2"]
```

<br/>

### in

expr1 in(expr2, expr3, ...) - Returns true if `expr` equals to any valN.

**Arguments:**

* expr1, expr2, expr3, ... - the arguments must be same type.

**Examples:**

```
> SELECT 1 in(1, 2, 3);
 true
> SELECT 1 in(2, 3, 4);
 false
> SELECT named_struct('a', 1, 'b', 2) in(named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3));
 false
> SELECT named_struct('a', 1, 'b', 2) in(named_struct('a', 1, 'b', 2), named_struct('a', 1, 'b', 3));
 true
```

<br/>

### initcap

initcap(str) - Returns `str` with the first letter of each word in uppercase.
All other letters are in lowercase. Words are delimited by white space.

**Examples:**

```
> SELECT initcap('sPark sql');
 Spark Sql
```

<br/>

### inline

inline(expr) - Explodes an array of structs into a table.

**Examples:**

```
> SELECT inline(array(struct(1, 'a'), struct(2, 'b')));
 1  a
 2  b
```

<br/>

### inline_outer

inline_outer(expr) - Explodes an array of structs into a table.

**Examples:**

```
> SELECT inline_outer(array(struct(1, 'a'), struct(2, 'b')));
 1  a
 2  b
```

<br/>

### input_file_block_length

input_file_block_length() - Returns the length of the block being read, or -1 if not available.

<br/>

### input_file_block_start

input_file_block_start() - Returns the start offset of the block being read, or -1 if not available.

<br/>

### input_file_name

input_file_name() - Returns the name of the file being read, or empty string if not available.

<br/>

### instr

instr(str, substr) - Returns the (1-based) index of the first occurrence of `substr` in `str`.

**Examples:**

```
> SELECT instr('SparkSQL', 'SQL');
 6
```

<br/>

### int

int(expr) - Casts the value `expr` to the target data type `int`.

<br/>

### isnan

isnan(expr) - Returns true if `expr` is NaN, or false otherwise.

**Examples:**

```
> SELECT isnan(cast('NaN' as double));
 true
```

<br/>

### isnotnull

isnotnull(expr) - Returns true if `expr` is not null, or false otherwise.

**Examples:**

```
> SELECT isnotnull(1);
 true
```

<br/>

### isnull

isnull(expr) - Returns true if `expr` is null, or false otherwise.

**Examples:**

```
> SELECT isnull(1);
 false
```

<br/>

### java_method

java_method(class, method[, arg1[, arg2 ..]]) - Calls a method with reflection.

**Examples:**

```
> SELECT java_method('java.util.UUID', 'randomUUID');
 c33fb387-8500-4bfa-81d2-6e0e3e930df2
> SELECT java_method('java.util.UUID', 'fromString', 'a5cf6c42-0c85-418f-af6c-3e4e5b1328f2');
 a5cf6c42-0c85-418f-af6c-3e4e5b1328f2
```

<br/>

### json_tuple

json_tuple(jsonStr, p1, p2, ..., pn) - Returns a tuple like the function get_json_object, but it takes multiple names. All the input parameters and output column types are string.

**Examples:**

```
> SELECT json_tuple('{"a":1, "b":2}', 'a', 'b');
 1  2
```

<br/>

### kurtosis

kurtosis(expr) - Returns the kurtosis value calculated from values of a group.

<br/>

### lag

lag(input[, offset[, default]]) - Returns the value of `input` at the `offset`th row
before the current row in the window. The default value of `offset` is 1 and the default
value of `default` is null. If the value of `input` at the `offset`th row is null,
null is returned. If there is no such offset row (e.g., when the offset is 1, the first
row of the window does not have any previous row), `default` is returned.

<br/>

### last

last(expr[, isIgnoreNull]) - Returns the last value of `expr` for a group of rows.
If `isIgnoreNull` is true, returns only non-null values.

<br/>

### last_day

last_day(date) - Returns the last day of the month which the date belongs to.

**Examples:**

```
> SELECT last_day('2009-01-12');
 2009-01-31
```

**Since:** 1.5.0

<br/>

### last_value

last_value(expr[, isIgnoreNull]) - Returns the last value of `expr` for a group of rows.
If `isIgnoreNull` is true, returns only non-null values.

<br/>

### lcase

lcase(str) - Returns `str` with all characters changed to lowercase.

**Examples:**

```
> SELECT lcase('SparkSql');
 sparksql
```

<br/>

### lead

lead(input[, offset[, default]]) - Returns the value of `input` at the `offset`th row
after the current row in the window. The default value of `offset` is 1 and the default
value of `default` is null. If the value of `input` at the `offset`th row is null,
null is returned. If there is no such an offset row (e.g., when the offset is 1, the last
row of the window does not have any subsequent row), `default` is returned.

<br/>

### least

least(expr, ...) - Returns the least value of all parameters, skipping null values.

**Examples:**

```
> SELECT least(10, 9, 2, 4, 3);
 2
```

<br/>

### left

left(str, len) - Returns the leftmost `len`(`len` can be string type) characters from the string `str`,if `len` is less or equal than 0 the result is an empty string.

**Examples:**

```
> SELECT left('Spark SQL', 3);
 Spa
```

<br/>

### length

length(expr) - Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.

**Examples:**

```
> SELECT length('Spark SQL ');
 10
> SELECT CHAR_LENGTH('Spark SQL ');
 10
> SELECT CHARACTER_LENGTH('Spark SQL ');
 10
```

<br/>

### levenshtein

levenshtein(str1, str2) - Returns the Levenshtein distance between the two given strings.

**Examples:**

```
> SELECT levenshtein('kitten', 'sitting');
 3
```

<br/>

### like

str like pattern - Returns true if str matches pattern, null if any arguments are null, false otherwise.

**Arguments:**

* str - a string expression
* pattern - a string expression. The pattern is a string which is matched literally, with
    exception to the following special symbols:

    _ matches any one character in the input (similar to . in posix regular expressions)

    % matches zero or more characters in the input (similar to .* in posix regular
    expressions)

    The escape character is '\'. If an escape character precedes a special symbol or another
    escape character, the following character is matched literally. It is invalid to escape
    any other character.

    Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
    to match "\abc", the pattern should be "\\abc".

    When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it fallbacks
    to Spark 1.6 behavior regarding string literal parsing. For example, if the config is
    enabled, the pattern to match "\abc" should be "\abc".

**Examples:**

```
> SELECT '%SystemDrive%\Users\John' like '\%SystemDrive\%\\Users%'
true
```

**Note:**

Use RLIKE to match with standard regular expressions.

<br/>

### ln

ln(expr) - Returns the natural logarithm (base e) of `expr`.

**Examples:**

```
> SELECT ln(1);
 0.0
```

<br/>

### locate

locate(substr, str[, pos]) - Returns the position of the first occurrence of `substr` in `str` after position `pos`.
The given `pos` and return value are 1-based.

**Examples:**

```
> SELECT locate('bar', 'foobarbar');
 4
> SELECT locate('bar', 'foobarbar', 5);
 7
> SELECT POSITION('bar' IN 'foobarbar');
 4
```

<br/>

### log

log(base, expr) - Returns the logarithm of `expr` with `base`.

**Examples:**

```
> SELECT log(10, 100);
 2.0
```

<br/>

### log10

log10(expr) - Returns the logarithm of `expr` with base 10.

**Examples:**

```
> SELECT log10(10);
 1.0
```

<br/>

### log1p

log1p(expr) - Returns log(1 + `expr`).

**Examples:**

```
> SELECT log1p(0);
 0.0
```

<br/>

### log2

log2(expr) - Returns the logarithm of `expr` with base 2.

**Examples:**

```
> SELECT log2(2);
 1.0
```

<br/>

### lower

lower(str) - Returns `str` with all characters changed to lowercase.

**Examples:**

```
> SELECT lower('SparkSql');
 sparksql
```

<br/>

### lpad

lpad(str, len, pad) - Returns `str`, left-padded with `pad` to a length of `len`.
If `str` is longer than `len`, the return value is shortened to `len` characters.

**Examples:**

```
> SELECT lpad('hi', 5, '??');
 ???hi
> SELECT lpad('hi', 1, '??');
 h
```

<br/>

### ltrim

ltrim(str) - Removes the leading space characters from `str`.

ltrim(trimStr, str) - Removes the leading string contains the characters from the trim string

**Arguments:**

* str - a string expression
* trimStr - the trim string characters to trim, the default value is a single space

**Examples:**

```
> SELECT ltrim('    SparkSQL   ');
 SparkSQL
> SELECT ltrim('Sp', 'SSparkSQLS');
 arkSQLS
```

<br/>

### map

map(key0, value0, key1, value1, ...) - Creates a map with the given key/value pairs.

**Examples:**

```
> SELECT map(1.0, '2', 3.0, '4');
 {1.0:"2",3.0:"4"}
```

<br/>

### map_keys

map_keys(map) - Returns an unordered array containing the keys of the map.

**Examples:**

```
> SELECT map_keys(map(1, 'a', 2, 'b'));
 [1,2]
```

<br/>

### map_values

map_values(map) - Returns an unordered array containing the values of the map.

**Examples:**

```
> SELECT map_values(map(1, 'a', 2, 'b'));
 ["a","b"]
```

<br/>

### max

max(expr) - Returns the maximum value of `expr`.

<br/>

### md5

md5(expr) - Returns an MD5 128-bit checksum as a hex string of `expr`.

**Examples:**

```
> SELECT md5('Spark');
 8cde774d6f7333752ed72cacddb05126
```

<br/>

### mean

mean(expr) - Returns the mean calculated from values of a group.

<br/>

### min

min(expr) - Returns the minimum value of `expr`.

<br/>

### minute

minute(timestamp) - Returns the minute component of the string/timestamp.

**Examples:**

```
> SELECT minute('2009-07-30 12:58:59');
 58
```

**Since:** 1.5.0

<br/>

### mod

expr1 mod expr2 - Returns the remainder after `expr1`/`expr2`.

**Examples:**

```
> SELECT 2 mod 1.8;
 0.2
> SELECT MOD(2, 1.8);
 0.2
```

<br/>

### monotonically_increasing_id

monotonically_increasing_id() - Returns monotonically increasing 64-bit integers. The generated ID is guaranteed
to be monotonically increasing and unique, but not consecutive. The current implementation
puts the partition ID in the upper 31 bits, and the lower 33 bits represent the record number
within each partition. The assumption is that the data frame has less than 1 billion
partitions, and each partition has less than 8 billion records.

<br/>

### month

month(date) - Returns the month component of the date/timestamp.

**Examples:**

```
> SELECT month('2016-07-30');
 7
```

**Since:** 1.5.0

<br/>

### months_between

months_between(timestamp1, timestamp2) - Returns number of months between `timestamp1` and `timestamp2`.

**Examples:**

```
> SELECT months_between('1997-02-28 10:30:00', '1996-10-30');
 3.94959677
```

**Since:** 1.5.0

<br/>

### named_struct

named_struct(name1, val1, name2, val2, ...) - Creates a struct with the given field names and values.

**Examples:**

```
> SELECT named_struct("a", 1, "b", 2, "c", 3);
 {"a":1,"b":2,"c":3}
```

<br/>

### nanvl

nanvl(expr1, expr2) - Returns `expr1` if it's not NaN, or `expr2` otherwise.

**Examples:**

```
> SELECT nanvl(cast('NaN' as double), 123);
 123.0
```

<br/>

### negative

negative(expr) - Returns the negated value of `expr`.

**Examples:**

```
> SELECT negative(1);
 -1
```

<br/>

### next_day

next_day(start_date, day_of_week) - Returns the first date which is later than `start_date` and named as indicated.

**Examples:**

```
> SELECT next_day('2015-01-14', 'TU');
 2015-01-20
```

**Since:** 1.5.0

<br/>

### not

not expr - Logical not.

<br/>

### now

now() - Returns the current timestamp at the start of query evaluation.

**Since:** 1.5.0

<br/>

### ntile

ntile(n) - Divides the rows for each window partition into `n` buckets ranging
from 1 to at most `n`.

<br/>

### nullif

nullif(expr1, expr2) - Returns null if `expr1` equals to `expr2`, or `expr1` otherwise.

**Examples:**

```
> SELECT nullif(2, 2);
 NULL
```

<br/>

### nvl

nvl(expr1, expr2) - Returns `expr2` if `expr1` is null, or `expr1` otherwise.

**Examples:**

```
> SELECT nvl(NULL, array('2'));
 ["2"]
```

<br/>

### nvl2

nvl2(expr1, expr2, expr3) - Returns `expr2` if `expr1` is not null, or `expr3` otherwise.

**Examples:**

```
> SELECT nvl2(NULL, 2, 1);
 1
```

<br/>

### octet_length

octet_length(expr) - Returns the byte length of string data or number of bytes of binary data.

**Examples:**

```
> SELECT octet_length('Spark SQL');
 9
```

<br/>

### or

expr1 or expr2 - Logical OR.

<br/>

### parse_url

parse_url(url, partToExtract[, key]) - Extracts a part from a URL.

**Examples:**

```
> SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST')
 spark.apache.org
> SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY')
 query=1
> SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')
 1
```

<br/>

### percent_rank

percent_rank() - Computes the percentage ranking of a value in a group of values.

<br/>

### percentile

percentile(col, percentage [, frequency]) - Returns the exact percentile value of numeric column
`col` at the given percentage. The value of percentage must be between 0.0 and 1.0. The
value of frequency should be positive integral

percentile(col, array(percentage1 [, percentage2]...) [, frequency]) - Returns the exact
percentile value array of numeric column `col` at the given percentage(s). Each value
of the percentage array must be between 0.0 and 1.0. The value of frequency should be
positive integral

<br/>

### percentile_approx

percentile_approx(col, percentage [, accuracy]) - Returns the approximate percentile value of numeric
column `col` at the given percentage. The value of percentage must be between 0.0
and 1.0. The `accuracy` parameter (default: 10000) is a positive numeric literal which
controls approximation accuracy at the cost of memory. Higher value of `accuracy` yields
better accuracy, `1.0/accuracy` is the relative error of the approximation.
When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
In this case, returns the approximate percentile array of column `col` at the given
percentage array.

**Examples:**

```
> SELECT percentile_approx(10.0, array(0.5, 0.4, 0.1), 100);
 [10.0,10.0,10.0]
> SELECT percentile_approx(10.0, 0.5, 100);
 10.0
```

<br/>

### pi

pi() - Returns pi.

**Examples:**

```
> SELECT pi();
 3.141592653589793
```

<br/>

### pmod

pmod(expr1, expr2) - Returns the positive value of `expr1` mod `expr2`.

**Examples:**

```
> SELECT pmod(10, 3);
 1
> SELECT pmod(-10, 3);
 2
```

<br/>

### posexplode

posexplode(expr) - Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions.

**Examples:**

```
> SELECT posexplode(array(10,20));
 0  10
 1  20
```

<br/>

### posexplode_outer

posexplode_outer(expr) - Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions.

**Examples:**

```
> SELECT posexplode_outer(array(10,20));
 0  10
 1  20
```

<br/>

### position

position(substr, str[, pos]) - Returns the position of the first occurrence of `substr` in `str` after position `pos`.
The given `pos` and return value are 1-based.

**Examples:**

```
> SELECT position('bar', 'foobarbar');
 4
> SELECT position('bar', 'foobarbar', 5);
 7
> SELECT POSITION('bar' IN 'foobarbar');
 4
```

<br/>

### positive

positive(expr) - Returns the value of `expr`.

<br/>

### pow

pow(expr1, expr2) - Raises `expr1` to the power of `expr2`.

**Examples:**

```
> SELECT pow(2, 3);
 8.0
```

<br/>

### power

power(expr1, expr2) - Raises `expr1` to the power of `expr2`.

**Examples:**

```
> SELECT power(2, 3);
 8.0
```

<br/>

### printf

printf(strfmt, obj, ...) - Returns a formatted string from printf-style format strings.

**Examples:**

```
> SELECT printf("Hello World %d %s", 100, "days");
 Hello World 100 days
```

<br/>

### quarter

quarter(date) - Returns the quarter of the year for date, in the range 1 to 4.

**Examples:**

```
> SELECT quarter('2016-08-31');
 3
```

**Since:** 1.5.0

<br/>

### radians

radians(expr) - Converts degrees to radians.

**Arguments:**

* expr - angle in degrees

**Examples:**

```
> SELECT radians(180);
 3.141592653589793
```

<br/>

### rand

rand([seed]) - Returns a random value with independent and identically distributed (i.i.d.) uniformly distributed values in [0, 1).

**Examples:**

```
> SELECT rand();
 0.9629742951434543
> SELECT rand(0);
 0.8446490682263027
> SELECT rand(null);
 0.8446490682263027
```

<br/>

### randn

randn([seed]) - Returns a random value with independent and identically distributed (i.i.d.) values drawn from the standard normal distribution.

**Examples:**

```
> SELECT randn();
 -0.3254147983080288
> SELECT randn(0);
 1.1164209726833079
> SELECT randn(null);
 1.1164209726833079
```

<br/>

### rank

rank() - Computes the rank of a value in a group of values. The result is one plus the number
of rows preceding or equal to the current row in the ordering of the partition. The values
will produce gaps in the sequence.

<br/>

### reflect

reflect(class, method[, arg1[, arg2 ..]]) - Calls a method with reflection.

**Examples:**

```
> SELECT reflect('java.util.UUID', 'randomUUID');
 c33fb387-8500-4bfa-81d2-6e0e3e930df2
> SELECT reflect('java.util.UUID', 'fromString', 'a5cf6c42-0c85-418f-af6c-3e4e5b1328f2');
 a5cf6c42-0c85-418f-af6c-3e4e5b1328f2
```

<br/>

### regexp_extract

regexp_extract(str, regexp[, idx]) - Extracts a group that matches `regexp`.

**Examples:**

```
> SELECT regexp_extract('100-200', '(\d+)-(\d+)', 1);
 100
```

<br/>

### regexp_replace

regexp_replace(str, regexp, rep) - Replaces all substrings of `str` that match `regexp` with `rep`.

**Examples:**

```
> SELECT regexp_replace('100-200', '(\d+)', 'num');
 num-num
```

<br/>

### repeat

repeat(str, n) - Returns the string which repeats the given string value n times.

**Examples:**

```
> SELECT repeat('123', 2);
 123123
```

<br/>

### replace

replace(str, search[, replace]) - Replaces all occurrences of `search` with `replace`.

**Arguments:**

* str - a string expression
* search - a string expression. If `search` is not found in `str`, `str` is returned unchanged.
* replace - a string expression. If `replace` is not specified or is an empty string, nothing replaces
    the string that is removed from `str`.

**Examples:**

```
> SELECT replace('ABCabc', 'abc', 'DEF');
 ABCDEF
```

<br/>

### reverse

reverse(str) - Returns the reversed given string.

**Examples:**

```
> SELECT reverse('Spark SQL');
 LQS krapS
```

<br/>

### right

right(str, len) - Returns the rightmost `len`(`len` can be string type) characters from the string `str`,if `len` is less or equal than 0 the result is an empty string.

**Examples:**

```
> SELECT right('Spark SQL', 3);
 SQL
```

<br/>

### rint

rint(expr) - Returns the double value that is closest in value to the argument and is equal to a mathematical integer.

**Examples:**

```
> SELECT rint(12.3456);
 12.0
```

<br/>

### rlike

str rlike regexp - Returns true if `str` matches `regexp`, or false otherwise.

**Arguments:**

* str - a string expression
* regexp - a string expression. The pattern string should be a Java regular expression.

    Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
    parser. For example, to match "\abc", a regular expression for `regexp` can be
    "^\\abc$".

    There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
    fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
    if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".

**Examples:**

```
When spark.sql.parser.escapedStringLiterals is disabled (default).
> SELECT '%SystemDrive%\Users\John' rlike '%SystemDrive%\\Users.*'
true

When spark.sql.parser.escapedStringLiterals is enabled.
> SELECT '%SystemDrive%\Users\John' rlike '%SystemDrive%\Users.*'
true
```

**Note:**

Use LIKE to match with simple string pattern.

<br/>

### rollup

<br/>

### round

round(expr, d) - Returns `expr` rounded to `d` decimal places using HALF_UP rounding mode.

**Examples:**

```
> SELECT round(2.5, 0);
 3.0
```

<br/>

### row_number

row_number() - Assigns a unique, sequential number to each row, starting with one,
according to the ordering of rows within the window partition.

<br/>

### rpad

rpad(str, len, pad) - Returns `str`, right-padded with `pad` to a length of `len`.
If `str` is longer than `len`, the return value is shortened to `len` characters.

**Examples:**

```
> SELECT rpad('hi', 5, '??');
 hi???
> SELECT rpad('hi', 1, '??');
 h
```

<br/>

### rtrim

rtrim(str) - Removes the trailing space characters from `str`.

rtrim(trimStr, str) - Removes the trailing string which contains the characters from the trim string from the `str`

**Arguments:**

* str - a string expression
* trimStr - the trim string characters to trim, the default value is a single space

**Examples:**

```
> SELECT rtrim('    SparkSQL   ');
 SparkSQL
> SELECT rtrim('LQSa', 'SSparkSQLS');
 SSpark
```

<br/>

### second

second(timestamp) - Returns the second component of the string/timestamp.

**Examples:**

```
> SELECT second('2009-07-30 12:58:59');
 59
```

**Since:** 1.5.0

<br/>

### sentences

sentences(str[, lang, country]) - Splits `str` into an array of array of words.

**Examples:**

```
> SELECT sentences('Hi there! Good morning.');
 [["Hi","there"],["Good","morning"]]
```

<br/>

### sha

sha(expr) - Returns a sha1 hash value as a hex string of the `expr`.

**Examples:**

```
> SELECT sha('Spark');
 85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c
```

<br/>

### sha1

sha1(expr) - Returns a sha1 hash value as a hex string of the `expr`.

**Examples:**

```
> SELECT sha1('Spark');
 85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c
```

<br/>

### sha2

sha2(expr, bitLength) - Returns a checksum of SHA-2 family as a hex string of `expr`.
SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256.

**Examples:**

```
> SELECT sha2('Spark', 256);
 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
```

<br/>

### shiftleft

shiftleft(base, expr) - Bitwise left shift.

**Examples:**

```
> SELECT shiftleft(2, 1);
 4
```

<br/>

### shiftright

shiftright(base, expr) - Bitwise (signed) right shift.

**Examples:**

```
> SELECT shiftright(4, 1);
 2
```

<br/>

### shiftrightunsigned

shiftrightunsigned(base, expr) - Bitwise unsigned right shift.

**Examples:**

```
> SELECT shiftrightunsigned(4, 1);
 2
```

<br/>

### sign

sign(expr) - Returns -1.0, 0.0 or 1.0 as `expr` is negative, 0 or positive.

**Examples:**

```
> SELECT sign(40);
 1.0
```

<br/>

### signum

signum(expr) - Returns -1.0, 0.0 or 1.0 as `expr` is negative, 0 or positive.

**Examples:**

```
> SELECT signum(40);
 1.0
```

<br/>

### sin

sin(expr) - Returns the sine of `expr`, as if computed by `java.lang.Math.sin`.

**Arguments:**

* expr - angle in radians

**Examples:**

```
> SELECT sin(0);
 0.0
```

<br/>

### sinh

sinh(expr) - Returns hyperbolic sine of `expr`, as if computed by `java.lang.Math.sinh`.

**Arguments:**

* expr - hyperbolic angle

**Examples:**

```
> SELECT sinh(0);
 0.0
```

<br/>

### size

size(expr) - Returns the size of an array or a map. Returns -1 if null.

**Examples:**

```
> SELECT size(array('b', 'd', 'c', 'a'));
 4
```

<br/>

### skewness

skewness(expr) - Returns the skewness value calculated from values of a group.

<br/>

### smallint

smallint(expr) - Casts the value `expr` to the target data type `smallint`.

<br/>

### sort_array

sort_array(array[, ascendingOrder]) - Sorts the input array in ascending or descending order according to the natural ordering of the array elements.

**Examples:**

```
> SELECT sort_array(array('b', 'd', 'c', 'a'), true);
 ["a","b","c","d"]
```

<br/>

### soundex

soundex(str) - Returns Soundex code of the string.

**Examples:**

```
> SELECT soundex('Miller');
 M460
```

<br/>

### space

space(n) - Returns a string consisting of `n` spaces.

**Examples:**

```
> SELECT concat(space(2), '1');
   1
```

<br/>

### spark_partition_id

spark_partition_id() - Returns the current partition id.

<br/>

### split

split(str, regex) - Splits `str` around occurrences that match `regex`.

**Examples:**

```
> SELECT split('oneAtwoBthreeC', '[ABC]');
 ["one","two","three",""]
```

<br/>

### sqrt

sqrt(expr) - Returns the square root of `expr`.

**Examples:**

```
> SELECT sqrt(4);
 2.0
```

<br/>

### stack

stack(n, expr1, ..., exprk) - Separates `expr1`, ..., `exprk` into `n` rows.

**Examples:**

```
> SELECT stack(2, 1, 2, 3);
 1  2
 3  NULL
```

<br/>

### std

std(expr) - Returns the sample standard deviation calculated from values of a group.

<br/>

### stddev

stddev(expr) - Returns the sample standard deviation calculated from values of a group.

<br/>

### stddev_pop

stddev_pop(expr) - Returns the population standard deviation calculated from values of a group.

<br/>

### stddev_samp

stddev_samp(expr) - Returns the sample standard deviation calculated from values of a group.

<br/>

### str_to_map

str_to_map(text[, pairDelim[, keyValueDelim]]) - Creates a map after splitting the text into key/value pairs using delimiters. Default delimiters are ',' for `pairDelim` and ':' for `keyValueDelim`.

**Examples:**

```
> SELECT str_to_map('a:1,b:2,c:3', ',', ':');
 map("a":"1","b":"2","c":"3")
> SELECT str_to_map('a');
 map("a":null)
```

<br/>

### string

string(expr) - Casts the value `expr` to the target data type `string`.

<br/>

### struct

struct(col1, col2, col3, ...) - Creates a struct with the given field values.

<br/>

### substr

substr(str, pos[, len]) - Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of byte array that starts at `pos` and is of length `len`.

**Examples:**

```
> SELECT substr('Spark SQL', 5);
 k SQL
> SELECT substr('Spark SQL', -3);
 SQL
> SELECT substr('Spark SQL', 5, 1);
 k
```

<br/>

### substring

substring(str, pos[, len]) - Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of byte array that starts at `pos` and is of length `len`.

**Examples:**

```
> SELECT substring('Spark SQL', 5);
 k SQL
> SELECT substring('Spark SQL', -3);
 SQL
> SELECT substring('Spark SQL', 5, 1);
 k
```

<br/>

### substring_index

substring_index(str, delim, count) - Returns the substring from `str` before `count` occurrences of the delimiter `delim`.
If `count` is positive, everything to the left of the final delimiter (counting from the
left) is returned. If `count` is negative, everything to the right of the final delimiter
(counting from the right) is returned. The function substring_index performs a case-sensitive match
when searching for `delim`.

**Examples:**

```
> SELECT substring_index('www.apache.org', '.', 2);
 www.apache
```

<br/>

### sum

sum(expr) - Returns the sum calculated from values of a group.

<br/>

### tan

tan(expr) - Returns the tangent of `expr`, as if computed by `java.lang.Math.tan`.

**Arguments:**

* expr - angle in radians

**Examples:**

```
> SELECT tan(0);
 0.0
```

<br/>

### tanh

tanh(expr) - Returns the hyperbolic tangent of `expr`, as if computed by
`java.lang.Math.tanh`.

**Arguments:**

* expr - hyperbolic angle

**Examples:**

```
> SELECT tanh(0);
 0.0
```

<br/>

### timestamp

timestamp(expr) - Casts the value `expr` to the target data type `timestamp`.

<br/>

### tinyint

tinyint(expr) - Casts the value `expr` to the target data type `tinyint`.

<br/>

### to_date

to_date(date_str[, fmt]) - Parses the `date_str` expression with the `fmt` expression to
a date. Returns null with invalid input. By default, it follows casting rules to a date if
the `fmt` is omitted.

**Examples:**

```
> SELECT to_date('2009-07-30 04:17:52');
 2009-07-30
> SELECT to_date('2016-12-31', 'yyyy-MM-dd');
 2016-12-31
```

**Since:** 1.5.0

<br/>

### to_json

to_json(expr[, options]) - Returns a json string with a given struct value

**Examples:**

```
> SELECT to_json(named_struct('a', 1, 'b', 2));
 {"a":1,"b":2}
> SELECT to_json(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
 {"time":"26/08/2015"}
> SELECT to_json(array(named_struct('a', 1, 'b', 2));
 [{"a":1,"b":2}]
> SELECT to_json(map('a', named_struct('b', 1)));
 {"a":{"b":1}}
> SELECT to_json(map(named_struct('a', 1),named_struct('b', 2)));
 {"[1]":{"b":2}}
> SELECT to_json(map('a', 1));
 {"a":1}
> SELECT to_json(array((map('a', 1))));
 [{"a":1}]
```

**Since:** 2.2.0

<br/>

### to_timestamp

to_timestamp(timestamp[, fmt]) - Parses the `timestamp` expression with the `fmt` expression to
a timestamp. Returns null with invalid input. By default, it follows casting rules to
a timestamp if the `fmt` is omitted.

**Examples:**

```
> SELECT to_timestamp('2016-12-31 00:12:00');
 2016-12-31 00:12:00
> SELECT to_timestamp('2016-12-31', 'yyyy-MM-dd');
 2016-12-31 00:00:00
```

**Since:** 2.2.0

<br/>

### to_unix_timestamp

to_unix_timestamp(expr[, pattern]) - Returns the UNIX timestamp of the given time.

**Examples:**

```
> SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd');
 1460041200
```

**Since:** 1.6.0

<br/>

### to_utc_timestamp

to_utc_timestamp(timestamp, timezone) - Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield '2017-07-14 01:40:00.0'.

**Examples:**

```
> SELECT to_utc_timestamp('2016-08-31', 'Asia/Seoul');
 2016-08-30 15:00:00
```

**Since:** 1.5.0

<br/>

### translate

translate(input, from, to) - Translates the `input` string by replacing the characters present in the `from` string with the corresponding characters in the `to` string.

**Examples:**

```
> SELECT translate('AaBbCc', 'abc', '123');
 A1B2C3
```

<br/>

### trim

trim(str) - Removes the leading and trailing space characters from `str`.

trim(BOTH trimStr FROM str) - Remove the leading and trailing `trimStr` characters from `str`

trim(LEADING trimStr FROM str) - Remove the leading `trimStr` characters from `str`

trim(TRAILING trimStr FROM str) - Remove the trailing `trimStr` characters from `str`

**Arguments:**

* str - a string expression
* trimStr - the trim string characters to trim, the default value is a single space
* BOTH, FROM - these are keywords to specify trimming string characters from both ends of
    the string
* LEADING, FROM - these are keywords to specify trimming string characters from the left
    end of the string
* TRAILING, FROM - these are keywords to specify trimming string characters from the right
    end of the string

**Examples:**

```
> SELECT trim('    SparkSQL   ');
 SparkSQL
> SELECT trim('SL', 'SSparkSQLS');
 parkSQ
> SELECT trim(BOTH 'SL' FROM 'SSparkSQLS');
 parkSQ
> SELECT trim(LEADING 'SL' FROM 'SSparkSQLS');
 parkSQLS
> SELECT trim(TRAILING 'SL' FROM 'SSparkSQLS');
 SSparkSQ
```

<br/>

### trunc

trunc(date, fmt) - Returns `date` with the time portion of the day truncated to the unit specified by the format model `fmt`.
`fmt` should be one of ["year", "yyyy", "yy", "mon", "month", "mm"]

**Examples:**

```
> SELECT trunc('2009-02-12', 'MM');
 2009-02-01
> SELECT trunc('2015-10-27', 'YEAR');
 2015-01-01
```

**Since:** 1.5.0

<br/>

### ucase

ucase(str) - Returns `str` with all characters changed to uppercase.

**Examples:**

```
> SELECT ucase('SparkSql');
 SPARKSQL
```

<br/>

### unbase64

unbase64(str) - Converts the argument from a base 64 string `str` to a binary.

**Examples:**

```
> SELECT unbase64('U3BhcmsgU1FM');
 Spark SQL
```

<br/>

### unhex

unhex(expr) - Converts hexadecimal `expr` to binary.

**Examples:**

```
> SELECT decode(unhex('537061726B2053514C'), 'UTF-8');
 Spark SQL
```

<br/>

### unix_timestamp

unix_timestamp([expr[, pattern]]) - Returns the UNIX timestamp of current or specified time.

**Examples:**

```
> SELECT unix_timestamp();
 1476884637
> SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd');
 1460041200
```

**Since:** 1.5.0

<br/>

### upper

upper(str) - Returns `str` with all characters changed to uppercase.

**Examples:**

```
> SELECT upper('SparkSql');
 SPARKSQL
```

<br/>

### uuid

uuid() - Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.

**Examples:**

```
> SELECT uuid();
 46707d92-02f4-4817-8116-a4c3b23e6266
```

<br/>

### var_pop

var_pop(expr) - Returns the population variance calculated from values of a group.

<br/>

### var_samp

var_samp(expr) - Returns the sample variance calculated from values of a group.

<br/>

### variance

variance(expr) - Returns the sample variance calculated from values of a group.

<br/>

### weekofyear

weekofyear(date) - Returns the week of the year of the given date. A week is considered to start on a Monday and week 1 is the first week with >3 days.

**Examples:**

```
> SELECT weekofyear('2008-02-20');
 8
```

**Since:** 1.5.0

<br/>

### when

CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END - When `expr1` = true, returns `expr2`; else when `expr3` = true, returns `expr4`; else returns `expr5`.

**Arguments:**

* expr1, expr3 - the branch condition expressions should all be boolean type.
* expr2, expr4, expr5 - the branch value expressions and else value expression should all be
    same type or coercible to a common type.

**Examples:**

```
> SELECT CASE WHEN 1 > 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
 1
> SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
 2
> SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 < 0 THEN 2.0 END;
 NULL
```

<br/>

### window

<br/>

### xpath

xpath(xml, xpath) - Returns a string array of values within the nodes of xml that match the XPath expression.

**Examples:**

```
> SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b/text()');
 ['b1','b2','b3']
```

<br/>

### xpath_boolean

xpath_boolean(xml, xpath) - Returns true if the XPath expression evaluates to true, or if a matching node is found.

**Examples:**

```
> SELECT xpath_boolean('<a><b>1</b></a>','a/b');
 true
```

<br/>

### xpath_double

xpath_double(xml, xpath) - Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.

**Examples:**

```
> SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
 3.0
```

<br/>

### xpath_float

xpath_float(xml, xpath) - Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.

**Examples:**

```
> SELECT xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
 3.0
```

<br/>

### xpath_int

xpath_int(xml, xpath) - Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.

**Examples:**

```
> SELECT xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
 3
```

<br/>

### xpath_long

xpath_long(xml, xpath) - Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.

**Examples:**

```
> SELECT xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
 3
```

<br/>

### xpath_number

xpath_number(xml, xpath) - Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.

**Examples:**

```
> SELECT xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
 3.0
```

<br/>

### xpath_short

xpath_short(xml, xpath) - Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.

**Examples:**

```
> SELECT xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
 3
```

<br/>

### xpath_string

xpath_string(xml, xpath) - Returns the text contents of the first xml node that matches the XPath expression.

**Examples:**

```
> SELECT xpath_string('<a><b>b</b><c>cc</c></a>','a/c');
 cc
```

<br/>

### year

year(date) - Returns the year component of the date/timestamp.

**Examples:**

```
> SELECT year('2016-07-30');
 2016
```

**Since:** 1.5.0

<br/>

### |

expr1 | expr2 - Returns the result of bitwise OR of `expr1` and `expr2`.

**Examples:**

```
> SELECT 3 | 5;
 7
```

<br/>

### ~

~ expr - Returns the result of bitwise NOT of `expr`.

**Examples:**

```
> SELECT ~ 0;
 -1
```

<br/>

