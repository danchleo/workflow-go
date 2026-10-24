package db

// CoerceInt safely converts a value that survived JSON round-trip back to int.
// SQLStore uses a blob pattern: RuntimeContext / Context / Params maps are
// serialized as JSON TEXT columns and unmarshaled — Go ints become float64,
// int64s become float64, nested slices/maps are preserved. Any code reading
// a numeric field from these maps MUST go through CoerceInt rather than a
// raw type assertion.
func CoerceInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case int32:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	}
	return 0
}
