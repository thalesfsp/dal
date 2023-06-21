package insightly

import "time"

// CustomField definition.
type CustomField struct {
	FieldName  string      `json:"FIELD_NAME"`
	FieldValue interface{} `json:"FIELD_VALUE"`
}

// CreateContactRequest definition.
// https://api.insightly.com/v3.1/Help?_ga=2.265055051.556369518.1686759514-666734128.1686759514#!/Contacts/AddEntity
type CreateContactRequest struct {
	AddressMailCity      string        `json:"ADDRESS_MAIL_CITY"`
	AddressMailCountry   string        `json:"ADDRESS_MAIL_COUNTRY"`
	AddressMailPostcode  string        `json:"ADDRESS_MAIL_POSTCODE"`
	AddressMailState     string        `json:"ADDRESS_MAIL_STATE"`
	AddressMailStreet    string        `json:"ADDRESS_MAIL_STREET"`
	AddressOtherCity     string        `json:"ADDRESS_OTHER_CITY"`
	AddressOtherCountry  string        `json:"ADDRESS_OTHER_COUNTRY"`
	AddressOtherPostcode string        `json:"ADDRESS_OTHER_POSTCODE"`
	AddressOtherState    string        `json:"ADDRESS_OTHER_STATE"`
	AddressOtherStreet   string        `json:"ADDRESS_OTHER_STREET"`
	AssistantName        string        `json:"ASSISTANT_NAME"`
	Background           string        `json:"BACKGROUND"`
	ContactID            int           `json:"CONTACT_ID"`
	CustomFields         []CustomField `json:"CUSTOMFIELDS"`
	DateOfBirth          time.Time     `json:"DATE_OF_BIRTH"`
	EmailAddress         string        `json:"EMAIL_ADDRESS"`
	EmailOptedOut        bool          `json:"EMAIL_OPTED_OUT"`
	FirstName            string        `json:"FIRST_NAME"`
	LastName             string        `json:"LAST_NAME"`
	OrganisationID       int           `json:"ORGANISATION_ID"`
	OwnerUserID          int           `json:"OWNER_USER_ID"`
	Phone                string        `json:"PHONE"`
	PhoneAssistant       string        `json:"PHONE_ASSISTANT"`
	PhoneFax             string        `json:"PHONE_FAX"`
	PhoneHome            string        `json:"PHONE_HOME"`
	PhoneMobile          string        `json:"PHONE_MOBILE"`
	PhoneOther           string        `json:"PHONE_OTHER"`
	Salutation           string        `json:"SALUTATION"`
	SocialFacebook       string        `json:"SOCIAL_FACEBOOK"`
	SocialLinkedIn       string        `json:"SOCIAL_LINKEDIN"`
	SocialTwitter        string        `json:"SOCIAL_TWITTER"`
	Title                string        `json:"TITLE"`
}

// CreateContactResponse definition.
type CreateContactResponse struct {
	AddressMailCity      string    `json:"ADDRESS_MAIL_CITY"`
	AddressMailCountry   string    `json:"ADDRESS_MAIL_COUNTRY"`
	AddressMailPostcode  string    `json:"ADDRESS_MAIL_POSTCODE"`
	AddressMailState     string    `json:"ADDRESS_MAIL_STATE"`
	AddressMailStreet    string    `json:"ADDRESS_MAIL_STREET"`
	AddressOtherCity     string    `json:"ADDRESS_OTHER_CITY"`
	AddressOtherCountry  string    `json:"ADDRESS_OTHER_COUNTRY"`
	AddressOtherPostcode string    `json:"ADDRESS_OTHER_POSTCODE"`
	AddressOtherState    string    `json:"ADDRESS_OTHER_STATE"`
	AddressOtherStreet   string    `json:"ADDRESS_OTHER_STREET"`
	AssistantName        string    `json:"ASSISTANT_NAME"`
	Background           string    `json:"BACKGROUND"`
	ContactID            int       `json:"CONTACT_ID"`
	CreatedUserID        int       `json:"CREATED_USER_ID"`
	DateCreatedUtc       time.Time `json:"DATE_CREATED_UTC"`
	DateOfBirth          time.Time `json:"DATE_OF_BIRTH"`
	DateUpdatedUtc       time.Time `json:"DATE_UPDATED_UTC"`
	EmailAddress         string    `json:"EMAIL_ADDRESS"`
	EmailOptedOut        bool      `json:"EMAIL_OPTED_OUT"`
	FirstName            string    `json:"FIRST_NAME"`
	ImageURL             string    `json:"IMAGE_URL"`
	LastActivityDateUtc  time.Time `json:"LAST_ACTIVITY_DATE_UTC"`
	LastName             string    `json:"LAST_NAME"`
	NextActivityDateUtc  time.Time `json:"NEXT_ACTIVITY_DATE_UTC"`
	OrganisationID       int       `json:"ORGANISATION_ID"`
	OwnerUserID          int       `json:"OWNER_USER_ID"`
	Phone                string    `json:"PHONE"`
	PhoneAssistant       string    `json:"PHONE_ASSISTANT"`
	PhoneFax             string    `json:"PHONE_FAX"`
	PhoneHome            string    `json:"PHONE_HOME"`
	PhoneMobile          string    `json:"PHONE_MOBILE"`
	PhoneOther           string    `json:"PHONE_OTHER"`
	Salutation           string    `json:"SALUTATION"`
	SocialFacebook       string    `json:"SOCIAL_FACEBOOK"`
	SocialLinkedin       string    `json:"SOCIAL_LINKEDIN"`
	SocialTwitter        string    `json:"SOCIAL_TWITTER"`
	Title                string    `json:"TITLE"`

	Customfields []struct {
		FieldName  string `json:"FIELD_NAME"`
		FieldValue struct {
		} `json:"FIELD_VALUE"`
	} `json:"CUSTOMFIELDS"`

	Tags []struct {
		TagName string `json:"TAG_NAME"`
	} `json:"TAGS"`

	Dates []struct {
		CreateTaskYearly bool      `json:"CREATE_TASK_YEARLY"`
		DateID           int       `json:"DATE_ID"`
		OccasionDate     time.Time `json:"OCCASION_DATE"`
		OccasionName     string    `json:"OCCASION_NAME"`
		RepeatYearly     bool      `json:"REPEAT_YEARLY"`
	} `json:"DATES"`

	Links []struct {
		Details        string `json:"DETAILS"`
		IsForward      bool   `json:"IS_FORWARD"`
		LinkID         int    `json:"LINK_ID"`
		LinkObjectID   int    `json:"LINK_OBJECT_ID"`
		LinkObjectName string `json:"LINK_OBJECT_NAME"`
		ObjectID       int    `json:"OBJECT_ID"`
		ObjectName     string `json:"OBJECT_NAME"`
		RelationshipID int    `json:"RELATIONSHIP_ID"`
		Role           string `json:"ROLE"`
	} `json:"LINKS"`
}
