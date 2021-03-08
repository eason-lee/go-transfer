module go-transfer

go 1.15

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

require (
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/google/go-cmp v0.5.4 // indirect
	github.com/json-iterator/go v1.1.10
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/olivere/elastic v6.2.35+incompatible
	github.com/pkg/errors v0.9.1 // indirect
	github.com/segmentio/kafka-go v0.4.10
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20200625001655-4c5254603344 // indirect
	golang.org/x/sys v0.0.0-20201214210602-f9fddec55a1e // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.4.0
)
