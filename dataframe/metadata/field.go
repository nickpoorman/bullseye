package metadata

import "github.com/apache/arrow/go/arrow"

const (
	originalTypeKey = "BULLSEYE_ORIGINAL_TYPE"
	mapConstant     = "MAP"
	logicalTypeKey  = "LogicalType"
)

func AppendOriginalTypeMetadata(metadata arrow.Metadata, value string) arrow.Metadata {
	keys := append(metadata.Keys(), originalTypeKey, logicalTypeKey)
	values := append(metadata.Values(), value, value)
	return arrow.NewMetadata(keys, values)
}

func AppendOriginalMapTypeMetadata(metadata arrow.Metadata) arrow.Metadata {
	return AppendOriginalTypeMetadata(metadata, mapConstant)
}

func OriginalMapTypeMetadataExists(metadata arrow.Metadata) bool {
	if value, ok := metadataValue(metadata, logicalTypeKey); ok {
		return value == mapConstant
	}
	if value, ok := metadataValue(metadata, originalTypeKey); ok {
		return value == mapConstant
	}
	return false
}

func metadataValue(metadata arrow.Metadata, key string) (string, bool) {
	idx := metadata.FindKey(key)
	if idx == -1 {
		return "", false
	}
	return metadata.Values()[idx], true
}
