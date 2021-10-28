package drpc

import (
	"go.unistack.org/micro/v3/errors"
)

func microError(err error) error {
	// no error

	if err == nil {
		return nil
	}

	if verr, ok := err.(*errors.Error); ok {
		return verr
	}

	// fallback
	return &errors.Error{
		ID:     "go.micro.client",
		Code:   500,
		Detail: err.Error(),
		// Status: http.StatusInternalServerError,
	}
}
