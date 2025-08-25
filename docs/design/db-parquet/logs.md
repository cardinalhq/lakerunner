# Fields of the "cooked" db/ Parquet for logs

Log parquet files are a flattened version of the OpenTelemetry wire format
build for rapid searching.

## _cardinalhq prefix

Anything beginning with `_cardinalhq` is used by the CardinalHQ Lakerunner data lake
system.

`_cardinalhq.fingerprint` (int64) represents the message body content such that
messages which are similar to one another will have the same fingerprint.  Detectable items
like IP addresses, identifiers, process IDs, and other variable content are removed, and the
remaining string is fingeprinted.

`_cardinalhq.id` (string) is used by the CardinalHQ UI to identify this record.  It should be unique.

`_cardinalhq.level` (string) stores the uppercase log level.

`_cardinalhq.json` (string) may contain a string that contains json content if any was in the OTEL message body,
or the message body was itself a JSON object.  If it was, the field `_cardinalhq.json` may contain
the log message body after searching inside the JSON for common message keys.

`_cardinalhq.message` (string) contains the human (well, and now AI) readable content either from the OTEL `body`
log field, or one obtained from a JSON message body.  It may be blank.

`_cardinalhq.name` (string) is always set to the constant `log.events`.

`_cardinalhq.telemetry_type` (string) is always set to the constant `logs`.

`_cardinalhq.timestamp` (int64) is the microseconds since the Unix epoch.

`_cardinalhq.value` (float64) is no longer used, and should not be considered valid for logs.
This field should not be present in modern files.

## attributes

Resource, scope, and log attributes are stored with a prefix, and the values are always the string
representation of the value.

If a particular row has a `null` this usually means that record did not have that attribute
present, but some other row in the Parquet file does.

* `resource.*` are from the resource's attributes.
* `scope.*` are taken from the scope, including any schema if set.
* `log.*` are specific to that log record.
