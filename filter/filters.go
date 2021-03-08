package filter

import "go-transfer/config"

const (
	filterDropFields = "drop_field"
	filterUpdateFields    = "update_field"
)

type FilterFunc func(map[string]interface{}) map[string]interface{}


func CreateFilters(conf *config.ServerConf) []FilterFunc {
	var filters []FilterFunc

	for _, f := range conf.Filters {
		switch f.Action {
		case filterDropFields:
			filters = append(filters, DropFieldFilter(f.Fields))
		case filterUpdateFields:
			filters = append(filters, UpdateFieldFilter(f.Field, f.Target))
		}
	}

	return filters
}
