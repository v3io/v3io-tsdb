package v3io

import "strconv"

type Item map[string]interface{}

func (i Item) GetField(name string) interface{} {
	return i[name]
}

func (i Item) GetFieldInt(name string) (int, error) {
	switch typedField := i[name].(type) {
	case int:
		return typedField, nil
	case float64:
		return int(typedField), nil
	case string:
		return strconv.Atoi(typedField)
	default:
		return 0, ErrInvalidTypeConversion
	}
}

func (i Item) GetFieldString(name string) (string, error) {
	switch typedField := i[name].(type) {
	case int:
		return strconv.Itoa(typedField), nil
	case float64:
		return strconv.FormatFloat(typedField, 'E', -1, 64), nil
	case string:
		return typedField, nil
	default:
		return "", ErrInvalidTypeConversion
	}
}
