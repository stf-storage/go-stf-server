// +build redis

package config

import (
	"github.com/vmihailenco/redis/v2"
)

type QueueConfig redis.Options
