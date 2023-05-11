package mongodb

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// IDtoFilter converts an ID to a filter.
func IDtoFilter(id string) primitive.M {
	filter := bson.M{}

	// Only process the ID if it isn't a valid ID.
	if primitive.IsValidObjectID(id) {
		return bson.M{"_id": id}
	}

	// Try to convert the objectID to an ObjectID.
	finalID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		// If the objectID is not an ObjectID hexadecimal string, assume it's a plain string ID.
		filter["_id"] = id
	} else {
		// If the objectID is an ObjectID hexadecimal string, use the ObjectID in the filter.
		filter["_id"] = finalID
	}

	return filter
}
