package main

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

const FlagUltraHDUploads int64 = 16 // joke flag

const RestrictionUploading int64 = 32

const FlagBot int64 = 32

const emailGraceSeconds int64 = 14 * 86400

type User struct {
	Username      string `bson:"_id"`
	Flags         int64  `bson:"flags"`
	Created       int64  `bson:"created"`
	EmailVerified bool   `bson:"email_verified"`
	Ban      struct {
		State        string `bson:"state"`
		Restrictions int64  `bson:"restrictions"`
		Expires      int64  `bson:"expires"`
	} `bson:"ban"`
}

// Whether the account is currently banned from uploading files.
func (u *User) UploadsRestricted() bool {
	if u.Ban.State == "none" || u.Ban.State == "" {
		return false
	}
	if !strings.Contains(u.Ban.State, "perm") && u.Ban.Expires < time.Now().Unix() {
		return false
	}
	return u.Ban.Restrictions&RestrictionUploading == RestrictionUploading
}

func getUserByToken(token string) (*User, error) {
	var user User
	err := db.Collection("usersv0").FindOne(
		context.TODO(),
		bson.M{"tokens": token},
	).Decode(&user)
	return &user, err
}

// Whether the account may upload given the email verification rules: verified
// accounts and bots always can; accounts that predate verification can until
// the grace period (counted from when verification was introduced) is over.
func (u *User) EmailAllowsUploads() bool {
	if u.EmailVerified || u.Flags&FlagBot == FlagBot {
		return true
	}
	var rollout struct {
		Ts int64 `bson:"ts"`
	}
	err := db.Collection("server_config").FindOne(
		context.TODO(),
		bson.M{"_id": "email_rollout"},
	).Decode(&rollout)
	if err != nil {
		// verification hasn't been switched on yet
		return true
	}
	return u.Created < rollout.Ts && time.Now().Unix() < rollout.Ts+emailGraceSeconds
}
